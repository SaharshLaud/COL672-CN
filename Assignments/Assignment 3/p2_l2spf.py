from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types
import json
import heapq
import random

class ShortestPathSwitch(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ShortestPathSwitch, self).__init__(*args, **kwargs)
        self.topology = {}
        self.datapaths = {}
        self.host_location = {}  # Store MAC -> (switch_dpid, port)
        self.flows_installed = set()  # Track which flows we've installed
        self.load_config('config.json')

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
        return port_map.get((src_switch, dst_switch), None)

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

        # If we know both hosts' locations, install bidirectional flows
        if src in self.host_location and dst in self.host_location:
            flow_key = (src, dst)
            reverse_key = (dst, src)
            
            if flow_key not in self.flows_installed:
                src_dpid, src_port = self.host_location[src]
                dst_dpid, dst_port = self.host_location[dst]
                
                if src_dpid != dst_dpid:
                    src_switch = f's{src_dpid}'
                    dst_switch = f's{dst_dpid}'
                    
                    distance, paths = self.dijkstra(self.topology, src_switch, dst_switch)
                    
                    if paths:
                        # Select path based on ECMP setting
                        if self.ecmp_enabled and len(paths) > 1:
                            path = random.choice(paths)
                            self.logger.info("ECMP: Selected path %s from %d paths", path, len(paths))
                        else:
                            path = paths[0]
                        
                        self.logger.info("Selected path: %s for %s->%s", path, src, dst)
                        
                        # Install flows along the path (src -> dst)
                        for i in range(len(path) - 1):
                            curr_sw = path[i]
                            next_sw = path[i + 1]
                            curr_dpid = int(curr_sw[1:])
                            curr_datapath = self.datapaths.get(curr_dpid)
                            
                            if curr_datapath:
                                curr_parser = curr_datapath.ofproto_parser  # FIXED: Use correct parser
                                out_port = self.get_link_port(curr_sw, next_sw)
                                if out_port:
                                    match = curr_parser.OFPMatch(eth_dst=dst)  # FIXED: Match only on dst
                                    actions = [curr_parser.OFPActionOutput(out_port)]
                                    self.add_flow(curr_datapath, 10, match, actions, idle_timeout=300)
                        
                        # Final hop (src -> dst)
                        dst_datapath = self.datapaths.get(dst_dpid)
                        if dst_datapath:
                            dst_parser = dst_datapath.ofproto_parser  # FIXED: Use correct parser
                            match = dst_parser.OFPMatch(eth_dst=dst)  # FIXED: Match only on dst
                            actions = [dst_parser.OFPActionOutput(dst_port)]
                            self.add_flow(dst_datapath, 10, match, actions, idle_timeout=300)
                        
                        self.flows_installed.add(flow_key)
                        self.logger.info("Installed flows for %s -> %s", src, dst)
                        
                        # ADDED: Install reverse direction flows (dst -> src)
                        if reverse_key not in self.flows_installed:
                            self.logger.info("Installing reverse flows for %s -> %s", dst, src)
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
                                        match = curr_parser.OFPMatch(eth_dst=src)
                                        actions = [curr_parser.OFPActionOutput(out_port)]
                                        self.add_flow(curr_datapath, 10, match, actions, idle_timeout=300)
                            
                            # Final hop for reverse (dst -> src)
                            src_datapath = self.datapaths.get(src_dpid)
                            if src_datapath:
                                src_parser = src_datapath.ofproto_parser
                                match = src_parser.OFPMatch(eth_dst=src)
                                actions = [src_parser.OFPActionOutput(src_port)]
                                self.add_flow(src_datapath, 10, match, actions, idle_timeout=300)
                            
                            self.flows_installed.add(reverse_key)
                            self.logger.info("Installed reverse flows for %s -> %s", dst, src)

        # Send packet out (flood if unknown)
        if dst in self.host_location:
            dst_dpid, dst_port = self.host_location[dst]
            if dpid == dst_dpid:
                out_port = dst_port
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
