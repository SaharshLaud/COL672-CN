from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types
from ryu.lib.packet import ipv4
from ryu.lib.packet import tcp
from ryu.lib.packet import udp
from ryu.lib import hub
import json
import heapq
import random
import time


class WeightedLoadBalancingSwitch(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]


    def __init__(self, *args, **kwargs):
        super(WeightedLoadBalancingSwitch, self).__init__(*args, **kwargs)
        self.topology = {}
        self.datapaths = {}
        self.host_location = {}  # Store MAC -> (switch_dpid, port)
        self.flows_installed = set()  # Track which flows we've installed
        
        # NEW: Track link utilization
        self.link_stats = {}  # {(src_dpid, dst_dpid): {'tx_bytes': x, 'timestamp': t}}
        self.link_utilization = {}  # {(src_dpid, dst_dpid): utilization_percentage}
        self.port_to_neighbor = {}  # {(dpid, port): neighbor_dpid}
        
        self.load_config('config.json')
        
        # NEW: Start monitoring thread
        self.monitor_thread = hub.spawn(self._monitor)


    def load_config(self, config_file):
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            self.ecmp_enabled = config.get('ecmp', False)
            nodes = config.get('nodes', [])
            weight_matrix = config.get('weight_matrix', [])
            
            for i, node in enumerate(nodes):
                self.topology[node] = {}
                for j, weight in enumerate(weight_matrix[i]):
                    if weight > 0 and i != j:
                        self.topology[node][nodes[j]] = weight
            
            self.logger.info("Loaded topology: %s", self.topology)
            self.logger.info("ECMP enabled: %s", self.ecmp_enabled)
        except Exception as e:
            self.logger.error("Failed to load config: %s", e)


    def dijkstra(self, graph, start, end):
        pq = [(0, start, [start])]
        visited = {}
        shortest_distance = None
        all_shortest_paths = []
        
        while pq:
            dist, node, path = heapq.heappop(pq)
            
            if shortest_distance is not None and dist > shortest_distance:
                break
            
            if node == end:
                if shortest_distance is None:
                    shortest_distance = dist
                if dist == shortest_distance:
                    all_shortest_paths.append(path)
                continue
            
            if node in visited and visited[node] < dist:
                continue
            
            visited[node] = dist
            
            if node in graph:
                for neighbor, weight in graph[node].items():
                    if neighbor not in path:
                        new_dist = dist + weight
                        new_path = path + [neighbor]
                        heapq.heappush(pq, (new_dist, neighbor, new_path))
        
        return shortest_distance, all_shortest_paths


    def get_link_port(self, src_switch, dst_switch):
        port_map = {
            ('s1', 's2'): 2, ('s2', 's1'): 1,
            ('s1', 's3'): 3, ('s3', 's1'): 1,
            ('s2', 's4'): 2, ('s4', 's2'): 1,
            ('s3', 's5'): 2, ('s5', 's3'): 1,
            ('s4', 's6'): 2, ('s6', 's4'): 2,
            ('s5', 's6'): 2, ('s6', 's5'): 3,
        }
        
        # NEW: Build port to neighbor mapping
        src_dpid = int(src_switch[1:])
        dst_dpid = int(dst_switch[1:])
        port = port_map.get((src_switch, dst_switch), None)
        if port:
            self.port_to_neighbor[(src_dpid, port)] = dst_dpid
        
        return port


    # NEW: Monitor link statistics periodically
    def _monitor(self):
        while True:
            for dp in self.datapaths.values():
                self._request_stats(dp)
            hub.sleep(3)  # Poll every 3 seconds


    def _request_stats(self, datapath):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)


    # NEW: Handle port statistics reply
    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        
        for stat in body:
            port_no = stat.port_no
            
            # Skip special ports
            if port_no > 10000:
                continue
            
            # Get neighbor switch for this port
            neighbor_dpid = self.port_to_neighbor.get((dpid, port_no))
            if neighbor_dpid is None:
                continue
            
            link_key = (dpid, neighbor_dpid)
            current_time = time.time()
            
            # Calculate utilization based on byte count
            if link_key in self.link_stats:
                prev_bytes = self.link_stats[link_key]['tx_bytes']
                prev_time = self.link_stats[link_key]['timestamp']
                
                time_diff = current_time - prev_time
                byte_diff = stat.tx_bytes - prev_bytes
                
                if time_diff > 0:
                    # Calculate bandwidth in Mbps (assuming 10 Mbps links)
                    bits_per_sec = (byte_diff * 8) / time_diff
                    mbps = bits_per_sec / (1024 * 1024)
                    utilization = (mbps / 10.0) * 100  # Percentage of 10 Mbps
                    
                    self.link_utilization[link_key] = utilization
                    if utilization > 1.0:  # Only log significant utilization
                        self.logger.info("Link s%s->s%s: %.2f Mbps (%.1f%%)", 
                                        dpid, neighbor_dpid, mbps, utilization)
            
            # Update statistics
            self.link_stats[link_key] = {
                'tx_bytes': stat.tx_bytes,
                'timestamp': current_time
            }


    # NEW: Calculate path weight based on link utilization
    def calculate_path_weight(self, path):
        """
        Calculate path weight based on current link utilization.
        Lower weight = better (less loaded) path.
        """
        total_weight = 0.0
        
        for i in range(len(path) - 1):
            src_dpid = int(path[i][1:])
            dst_dpid = int(path[i+1][1:])
            
            link_key = (src_dpid, dst_dpid)
            utilization = self.link_utilization.get(link_key, 0.0)
            
            # Weight increases with utilization
            # Add base weight of 1 to avoid zero weights
            total_weight += (1.0 + utilization / 10.0)
        
        return total_weight


    # NEW: Select path using weighted random selection
    def select_path_weighted(self, paths):
        """
        Select a path using weighted random selection.
        Paths with lower utilization have higher probability.
        """
        if len(paths) == 1:
            return paths[0]
        
        # Calculate weights for all paths (lower is better)
        path_weights = []
        for path in paths:
            weight = self.calculate_path_weight(path)
            path_weights.append(weight)
        
        # Invert weights so lower utilization = higher probability
        # Use softmax-like transformation to get probabilities
        max_weight = max(path_weights)
        inverted_weights = [max_weight - w + 1 for w in path_weights]
        total = sum(inverted_weights)
        probabilities = [w / total for w in inverted_weights]
        
        # Log the selection process
        for i, (path, util_weight, prob) in enumerate(zip(paths, path_weights, probabilities)):
            self.logger.info("Path %d: %s - Util Weight: %.2f, Probability: %.2f%%", 
                           i, path, util_weight, prob * 100)
        
        # Select path based on probabilities
        rand_val = random.random()
        cumulative = 0.0
        for i, prob in enumerate(probabilities):
            cumulative += prob
            if rand_val <= cumulative:
                selected_path = paths[i]
                self.logger.info("WEIGHTED LB: Selected path %s with probability %.2f%%", 
                               selected_path, probabilities[i] * 100)
                return selected_path
        
        # Fallback (should not reach here)
        return paths[-1]


    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        dpid = datapath.id
        
        self.datapaths[dpid] = datapath
        
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                         ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)
        
        self.logger.info("Switch s%s connected", dpid)


    def add_flow(self, datapath, priority, match, actions, idle_timeout=0):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                               match=match, instructions=inst,
                               idle_timeout=idle_timeout)
        datapath.send_msg(mod)


    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']
        dpid = datapath.id
        
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]
        
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return
        
        dst = eth.dst
        src = eth.src
        
        # Learn source location ONLY if it's from port 1 (host ports on s1 and s6)
        if in_port == 1 and (dpid == 1 or dpid == 6):
            if src not in self.host_location:
                self.host_location[src] = (dpid, in_port)
                self.logger.info("Learned host %s on s%d port %d", src, dpid, in_port)
        
        # MODIFIED: Extract IP and transport layer info for per-flow load balancing
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        tcp_pkt = pkt.get_protocol(tcp.tcp)
        udp_pkt = pkt.get_protocol(udp.udp)
        
        # Create flow key based on 5-tuple (src_ip, dst_ip, proto, src_port, dst_port)
        flow_key = None
        if ip_pkt:
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            proto = ip_pkt.proto
            
            if tcp_pkt:
                src_port = tcp_pkt.src_port
                dst_port = tcp_pkt.dst_port
                flow_key = (src_ip, dst_ip, proto, src_port, dst_port)
            elif udp_pkt:
                src_port = udp_pkt.src_port
                dst_port = udp_pkt.dst_port
                flow_key = (src_ip, dst_ip, proto, src_port, dst_port)
            else:
                # For other IP protocols, use IP addresses only
                flow_key = (src_ip, dst_ip, proto, 0, 0)
        else:
            # Non-IP traffic, use MAC-based flow key
            flow_key = (src, dst, 0, 0, 0)
        
        reverse_flow_key = None
        if ip_pkt and (tcp_pkt or udp_pkt):
            if tcp_pkt:
                reverse_flow_key = (dst_ip, src_ip, proto, dst_port, src_port)
            elif udp_pkt:
                reverse_flow_key = (dst_ip, src_ip, proto, dst_port, src_port)
        else:
            reverse_flow_key = (dst, src, 0, 0, 0)
        
        # If we know both hosts' locations, install bidirectional flows
        if src in self.host_location and dst in self.host_location and flow_key not in self.flows_installed:
            src_dpid, src_port_loc = self.host_location[src]
            dst_dpid, dst_port_loc = self.host_location[dst]
            
            if src_dpid != dst_dpid:
                src_switch = f's{src_dpid}'
                dst_switch = f's{dst_dpid}'
                
                distance, paths = self.dijkstra(self.topology, src_switch, dst_switch)
                
                if paths:
                    # MODIFIED: Select path based on ECMP setting and load balancing
                    if self.ecmp_enabled and len(paths) > 1:
                        path = self.select_path_weighted(paths)  # NEW: Use weighted selection
                    else:
                        path = paths[0]
                    
                    if ip_pkt and (tcp_pkt or udp_pkt):
                        if tcp_pkt:
                            self.logger.info("New TCP flow: %s:%d -> %s:%d, Selected path: %s", 
                                           src_ip, tcp_pkt.src_port, dst_ip, tcp_pkt.dst_port, path)
                        else:
                            self.logger.info("New UDP flow: %s:%d -> %s:%d, Selected path: %s", 
                                           src_ip, udp_pkt.src_port, dst_ip, udp_pkt.dst_port, path)
                    
                    # Install flows along the path (src -> dst) with 5-tuple matching
                    for i in range(len(path) - 1):
                        curr_sw = path[i]
                        next_sw = path[i + 1]
                        curr_dpid = int(curr_sw[1:])
                        curr_datapath = self.datapaths.get(curr_dpid)
                        
                        if curr_datapath:
                            curr_parser = curr_datapath.ofproto_parser
                            out_port = self.get_link_port(curr_sw, next_sw)
                            
                            if out_port:
                                # Create match based on available info
                                if ip_pkt and tcp_pkt:
                                    match = curr_parser.OFPMatch(
                                        eth_type=0x0800,
                                        ipv4_src=src_ip,
                                        ipv4_dst=dst_ip,
                                        ip_proto=6,
                                        tcp_src=tcp_pkt.src_port,
                                        tcp_dst=tcp_pkt.dst_port
                                    )
                                elif ip_pkt and udp_pkt:
                                    match = curr_parser.OFPMatch(
                                        eth_type=0x0800,
                                        ipv4_src=src_ip,
                                        ipv4_dst=dst_ip,
                                        ip_proto=17,
                                        udp_src=udp_pkt.src_port,
                                        udp_dst=udp_pkt.dst_port
                                    )
                                else:
                                    # Fallback to MAC-based matching
                                    match = curr_parser.OFPMatch(eth_dst=dst)
                                
                                actions = [curr_parser.OFPActionOutput(out_port)]
                                self.add_flow(curr_datapath, 10, match, actions, idle_timeout=60)
                    
                    # Final hop (src -> dst)
                    dst_datapath = self.datapaths.get(dst_dpid)
                    if dst_datapath:
                        dst_parser = dst_datapath.ofproto_parser
                        if ip_pkt and tcp_pkt:
                            match = dst_parser.OFPMatch(
                                eth_type=0x0800,
                                ipv4_src=src_ip,
                                ipv4_dst=dst_ip,
                                ip_proto=6,
                                tcp_src=tcp_pkt.src_port,
                                tcp_dst=tcp_pkt.dst_port
                            )
                        elif ip_pkt and udp_pkt:
                            match = dst_parser.OFPMatch(
                                eth_type=0x0800,
                                ipv4_src=src_ip,
                                ipv4_dst=dst_ip,
                                ip_proto=17,
                                udp_src=udp_pkt.src_port,
                                udp_dst=udp_pkt.dst_port
                            )
                        else:
                            match = dst_parser.OFPMatch(eth_dst=dst)
                        
                        actions = [dst_parser.OFPActionOutput(dst_port_loc)]
                        self.add_flow(dst_datapath, 10, match, actions, idle_timeout=60)
                    
                    self.flows_installed.add(flow_key)
                    
                    # Install reverse direction flows (dst -> src)
                    if reverse_flow_key not in self.flows_installed:
                        reverse_path = list(reversed(path))
                        
                        # Install flows along reverse path
                        for i in range(len(reverse_path) - 1):
                            curr_sw = reverse_path[i]
                            next_sw = reverse_path[i + 1]
                            curr_dpid = int(curr_sw[1:])
                            curr_datapath = self.datapaths.get(curr_dpid)
                            
                            if curr_datapath:
                                curr_parser = curr_datapath.ofproto_parser
                                out_port = self.get_link_port(curr_sw, next_sw)
                                
                                if out_port:
                                    if ip_pkt and tcp_pkt:
                                        match = curr_parser.OFPMatch(
                                            eth_type=0x0800,
                                            ipv4_src=dst_ip,
                                            ipv4_dst=src_ip,
                                            ip_proto=6,
                                            tcp_src=tcp_pkt.dst_port,
                                            tcp_dst=tcp_pkt.src_port
                                        )
                                    elif ip_pkt and udp_pkt:
                                        match = curr_parser.OFPMatch(
                                            eth_type=0x0800,
                                            ipv4_src=dst_ip,
                                            ipv4_dst=src_ip,
                                            ip_proto=17,
                                            udp_src=udp_pkt.dst_port,
                                            udp_dst=udp_pkt.src_port
                                        )
                                    else:
                                        match = curr_parser.OFPMatch(eth_dst=src)
                                    
                                    actions = [curr_parser.OFPActionOutput(out_port)]
                                    self.add_flow(curr_datapath, 10, match, actions, idle_timeout=60)
                        
                        # Final hop for reverse (dst -> src)
                        src_datapath = self.datapaths.get(src_dpid)
                        if src_datapath:
                            src_parser = src_datapath.ofproto_parser
                            if ip_pkt and tcp_pkt:
                                match = src_parser.OFPMatch(
                                    eth_type=0x0800,
                                    ipv4_src=dst_ip,
                                    ipv4_dst=src_ip,
                                    ip_proto=6,
                                    tcp_src=tcp_pkt.dst_port,
                                    tcp_dst=tcp_pkt.src_port
                                )
                            elif ip_pkt and udp_pkt:
                                match = src_parser.OFPMatch(
                                    eth_type=0x0800,
                                    ipv4_src=dst_ip,
                                    ipv4_dst=src_ip,
                                    ip_proto=17,
                                    udp_src=udp_pkt.dst_port,
                                    udp_dst=udp_pkt.src_port
                                )
                            else:
                                match = src_parser.OFPMatch(eth_dst=src)
                            
                            actions = [src_parser.OFPActionOutput(src_port_loc)]
                            self.add_flow(src_datapath, 10, match, actions, idle_timeout=60)
                        
                        self.flows_installed.add(reverse_flow_key)
        
        # Send packet out (flood if unknown)
        if dst in self.host_location:
            dst_dpid, dst_port_loc = self.host_location[dst]
            if dpid == dst_dpid:
                out_port = dst_port_loc
            else:
                out_port = ofproto.OFPP_FLOOD
        else:
            out_port = ofproto.OFPP_FLOOD
        
        actions = [parser.OFPActionOutput(out_port)]
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                 in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)
