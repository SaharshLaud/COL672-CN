from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, ether_types, arp, ipv4
import json
import heapq


class L3ShortestPathSwitch(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(L3ShortestPathSwitch, self).__init__(*args, **kwargs)
        self.topology = {}
        self.datapaths = {}
        self.switch_info = {}
        self.host_info = {}
        self.port_to_mac = {}
        self.port_name_to_num = {}
        self.flows_installed = set()
        self.load_config('p3_config.json')

    def load_config(self, config_file):
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
                
                for host in config.get('hosts', []):
                    self.host_info[host['ip']] = host
                    self.logger.info("Loaded host: %s at %s (switch=%s)", 
                                   host['name'], host['ip'], host['switch'])
                
                for switch in config.get('switches', []):
                    dpid = switch['dpid']
                    self.switch_info[dpid] = switch
                    self.logger.info("Loaded switch: %s (dpid=%d)", switch['name'], dpid)
                
                for link in config.get('links', []):
                    src = link['src']
                    dst = link['dst']
                    cost = link['cost']
                    
                    if src not in self.topology:
                        self.topology[src] = {}
                    self.topology[src][dst] = cost
                
                self.logger.info("Topology graph: %s", self.topology)
        except Exception as e:
            self.logger.error("Failed to load config: %s", e)

    def dijkstra(self, graph, start, end):
        """Compute shortest path using Dijkstra's algorithm"""
        pq = [(0, start, [start])]
        visited = set()
        
        while pq:
            dist, node, path = heapq.heappop(pq)
            
            if node in visited:
                continue
            
            visited.add(node)
            
            if node == end:
                return dist, path
            
            for neighbor, weight in graph.get(node, {}).items():
                if neighbor not in visited:
                    new_dist = dist + weight
                    new_path = path + [neighbor]
                    heapq.heappush(pq, (new_dist, neighbor, new_path))
        
        return None, None

    def get_switch_name(self, dpid):
        """Get switch name from dpid"""
        if dpid in self.switch_info:
            return self.switch_info[dpid]['name']
        return None

    def get_interface_by_neighbor(self, dpid, neighbor_name):
        """Get interface info by neighbor name"""
        if dpid not in self.switch_info:
            return None
        
        for intf in self.switch_info[dpid]['interfaces']:
            if intf['neighbor'] == neighbor_name:
                return intf
        return None

    def get_port_by_interface_name(self, dpid, intf_name):
        """Get port number from interface name"""
        if dpid in self.port_name_to_num:
            return self.port_name_to_num[dpid].get(intf_name)
        return None

    def get_mac_by_port(self, dpid, port_num):
        """Get MAC address from port number"""
        if dpid in self.port_to_mac:
            return self.port_to_mac[dpid].get(port_num)
        return None

    def ip_to_mac(self, ip_str):
        """Resolve IP to MAC address"""
        if ip_str in self.host_info:
            return self.host_info[ip_str]['mac']
        
        for dpid, switch in self.switch_info.items():
            for intf in switch['interfaces']:
                if intf['ip'] == ip_str:
                    if intf.get('mac') and intf['mac'] != 'auto':
                        return intf['mac']
                    
                    port_num = self.get_port_by_interface_name(dpid, intf['name'])
                    if port_num:
                        mac = self.get_mac_by_port(dpid, port_num)
                        if mac:
                            return mac
        
        return None

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
        
        self.send_port_desc_stats_request(datapath)
        
        switch_name = self.get_switch_name(dpid)
        self.logger.info("Switch %s (dpid=%d) connected", switch_name, dpid)

    def send_port_desc_stats_request(self, datapath):
        """Request port descriptions from switch"""
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        req = parser.OFPPortDescStatsRequest(datapath, 0)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPPortDescStatsReply, MAIN_DISPATCHER)
    def port_desc_stats_reply_handler(self, ev):
        """Build port mappings from port descriptions"""
        dpid = ev.msg.datapath.id
        
        if dpid not in self.port_name_to_num:
            self.port_name_to_num[dpid] = {}
        if dpid not in self.port_to_mac:
            self.port_to_mac[dpid] = {}
        
        for port in ev.msg.body:
            if port.port_no != 0xfffffffe:
                port_name = port.name.decode('utf-8')
                port_no = port.port_no
                port_mac = port.hw_addr
                
                self.port_name_to_num[dpid][port_name] = port_no
                self.port_to_mac[dpid][port_no] = port_mac
                
                self.logger.info("Port mapping: %s = port %d (MAC: %s)", 
                               port_name, port_no, port_mac)

    def add_flow(self, datapath, priority, match, actions, idle_timeout=0):
        """Add flow entry to switch"""
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
        in_port = msg.match['in_port']
        
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]
        
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return
        
        arp_pkt = pkt.get_protocol(arp.arp)
        if arp_pkt:
            self.handle_arp(datapath, in_port, eth, arp_pkt)
            return
        
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        if ip_pkt:
            self.handle_ip(datapath, in_port, eth, ip_pkt, msg)
            return

    def handle_arp(self, datapath, in_port, eth, arp_pkt):
        """Handle ARP requests by proxying replies"""
        dpid = datapath.id
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        if arp_pkt.opcode == arp.ARP_REQUEST:
            target_ip = arp_pkt.dst_ip
            target_mac = self.ip_to_mac(target_ip)
            
            if target_mac:
                self.logger.info("ARP Request for %s -> replying with %s", 
                               target_ip, target_mac)
                
                arp_reply = packet.Packet()
                arp_reply.add_protocol(ethernet.ethernet(
                    ethertype=ether_types.ETH_TYPE_ARP,
                    dst=eth.src,
                    src=target_mac))
                arp_reply.add_protocol(arp.arp(
                    opcode=arp.ARP_REPLY,
                    src_mac=target_mac,
                    src_ip=target_ip,
                    dst_mac=arp_pkt.src_mac,
                    dst_ip=arp_pkt.src_ip))
                arp_reply.serialize()
                
                actions = [parser.OFPActionOutput(in_port)]
                out = parser.OFPPacketOut(
                    datapath=datapath,
                    buffer_id=ofproto.OFP_NO_BUFFER,
                    in_port=ofproto.OFPP_CONTROLLER,
                    actions=actions,
                    data=arp_reply.data)
                datapath.send_msg(out)
            else:
                self.logger.warning("ARP Request for unknown IP: %s", target_ip)

    def handle_ip(self, datapath, in_port, eth, ip_pkt, msg):
        """Handle IP packets and install flows only at source switch"""
        dpid = datapath.id
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        src_ip = ip_pkt.src
        dst_ip = ip_pkt.dst
        current_switch = self.get_switch_name(dpid)
        
        self.logger.info("IP packet at switch %s: %s -> %s (TTL=%d)",
                        current_switch, src_ip, dst_ip, ip_pkt.ttl)
        
        if dst_ip.startswith('224.') or dst_ip.startswith('255.'):
            return
        
        if ip_pkt.ttl <= 1:
            self.logger.info("TTL expired, dropping packet")
            return
        
        flow_key = (src_ip, dst_ip)
        
        if flow_key in self.flows_installed:
            return
        
        src_switch = None
        for host_ip, host_cfg in self.host_info.items():
            if host_ip == src_ip:
                src_switch = host_cfg['switch']
                break
        
        if current_switch == src_switch:
            if self.install_path_flows(src_ip, dst_ip):
                self.flows_installed.add(flow_key)
                
                import time
                time.sleep(0.1)
                
                out = parser.OFPPacketOut(
                    datapath=datapath,
                    buffer_id=ofproto.OFP_NO_BUFFER,
                    in_port=in_port,
                    actions=[parser.OFPActionOutput(ofproto.OFPP_TABLE)],
                    data=msg.data)
                datapath.send_msg(out)
                self.logger.info("Re-injected first packet")

    def install_path_flows(self, src_ip, dst_ip):
        """Install bidirectional flows for communication between src_ip and dst_ip"""
        src_switch = None
        dst_switch = None
        
        for host_ip, host_cfg in self.host_info.items():
            if host_ip == src_ip:
                src_switch = host_cfg['switch']
            if host_ip == dst_ip:
                dst_switch = host_cfg['switch']
        
        if not src_switch or not dst_switch:
            self.logger.error("Cannot find switches for %s <-> %s", src_ip, dst_ip)
            return False
        
        distance, path = self.dijkstra(self.topology, src_switch, dst_switch)
        
        if not path:
            self.logger.error("No path found from %s to %s", src_switch, dst_switch)
            return False
        
        self.logger.info("Installing flows for path: %s (cost=%d)", path, distance)
        
        self._install_unidirectional_flows(path, src_ip, dst_ip)
        
        reverse_path = list(reversed(path))
        self._install_unidirectional_flows(reverse_path, dst_ip, src_ip)
        
        return True

    def _install_unidirectional_flows(self, path, src_ip, dst_ip):
        """Install flows along a path for src_ip -> dst_ip"""
        for i in range(len(path)):
            switch_name = path[i]
            
            switch_dpid = None
            for did, cfg in self.switch_info.items():
                if cfg['name'] == switch_name:
                    switch_dpid = did
                    break
            
            if not switch_dpid or switch_dpid not in self.datapaths:
                continue
            
            sw_datapath = self.datapaths[switch_dpid]
            sw_parser = sw_datapath.ofproto_parser
            
            if i < len(path) - 1:
                next_switch = path[i + 1]
                out_intf = self.get_interface_by_neighbor(switch_dpid, next_switch)
                
                next_dpid = None
                for did, cfg in self.switch_info.items():
                    if cfg['name'] == next_switch:
                        next_dpid = did
                        break
                
                if next_dpid:
                    in_intf_next = self.get_interface_by_neighbor(next_dpid, switch_name)
                    if in_intf_next:
                        next_hop_mac = self.ip_to_mac(in_intf_next['ip'])
                    else:
                        next_hop_mac = None
                else:
                    next_hop_mac = None
            else:
                host_neighbor = None
                for host_ip, host_cfg in self.host_info.items():
                    if host_ip == dst_ip:
                        if host_cfg['switch'] == switch_name:
                            host_neighbor = host_cfg['name']
                            break
                
                if not host_neighbor:
                    self.logger.error("Final switch %s not connected to %s", 
                                    switch_name, dst_ip)
                    continue
                
                out_intf = self.get_interface_by_neighbor(switch_dpid, host_neighbor)
                next_hop_mac = self.ip_to_mac(dst_ip)
            
            if not out_intf:
                self.logger.error("Cannot find output interface on %s", switch_name)
                continue
            
            if not next_hop_mac:
                self.logger.error("Cannot determine next hop MAC for %s", switch_name)
                continue
            
            out_port = self.get_port_by_interface_name(switch_dpid, out_intf['name'])
            if not out_port:
                self.logger.error("Cannot find port number for %s", out_intf['name'])
                continue
            
            src_mac = self.ip_to_mac(out_intf['ip'])
            if not src_mac:
                src_mac = self.get_mac_by_port(switch_dpid, out_port)
            
            if not src_mac:
                self.logger.error("Cannot determine source MAC for interface %s", out_intf['name'])
                continue
            
            match = sw_parser.OFPMatch(
                eth_type=ether_types.ETH_TYPE_IP,
                ipv4_dst=dst_ip)
            
            actions = [
                sw_parser.OFPActionDecNwTtl(),
                sw_parser.OFPActionSetField(eth_src=src_mac),
                sw_parser.OFPActionSetField(eth_dst=next_hop_mac),
                sw_parser.OFPActionOutput(out_port)
            ]
            
            self.add_flow(sw_datapath, 10, match, actions, idle_timeout=300)
            
            self.logger.info("Flow on %s: dst=%s -> port=%d (eth_src=%s, eth_dst=%s)",
                           switch_name, dst_ip, out_port, src_mac, next_hop_mac)