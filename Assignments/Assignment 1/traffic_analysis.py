# Assignment 1 COL 334/672 Computer Networks
# Saharsh Laud 2024MCS2002


import sys
import argparse
import matplotlib.pyplot as plt
from scapy.all import *
from scapy.layers.inet import IP, TCP
from scapy.layers.inet6 import IPv6


# First parse the pcap
def open_pcap(file):
    pcaket = rdpcap(file)
    return pcaket

# Isolate traffic corresponding to given webpage
def filter_pcaket(pkts, clt_ip, srvr_ip):
    final = []
    for pkt in pkts:
        if (IP in pkt or IPv6 in pkt) and TCP in pkt: # Only TCP packets as per Piazza (IPv4 or IPv6)
            ip = pkt[IP] if IP in pkt else pkt[IPv6]
            if ((ip.src==clt_ip and ip.dst==srvr_ip) or (ip.src==srvr_ip and ip.dst==clt_ip)):
                final.append(pkt)
    return final

# Find the throughput in one second window for download/upload
def throughput(pkts, clt_ip, srvr_ip, dir):
    one_sec = {}
    for pkt in pkts:
        if (IP in pkt or IPv6 in pkt) and TCP in pkt:
            ip = pkt[IP] if IP in pkt else pkt[IPv6]
            time = int(pkt.time)


            if (dir == "down" and ip.src == srvr_ip and ip.dst == clt_ip):
                one_sec[time] = one_sec.get(time, 0) + len(pkt)
            
            elif (dir == "up" and ip.src == clt_ip and ip.dst == srvr_ip):
                one_sec[time] = one_sec.get(time, 0) + len(pkt)
    
    if not one_sec: return [], []
    times = sorted(one_sec.keys())
    start_time = times[0]
    times = [t - start_time for t in times] # Start from 0
    throughput = [one_sec[t + start_time] * 8 for t in times] # Convert to bits
    
    return times, throughput

def rtt(pkts, clt_ip, srvr_ip):
    rtts = []
    ack_times = []
    sent_pkts = {}


    for pkt in pkts:
        if (IP in pkt or IPv6 in pkt) and TCP in pkt:
            ip = pkt[IP] if IP in pkt else pkt[IPv6]
            tcp = pkt[TCP]
            
            if ip.src == clt_ip and ip.dst == srvr_ip:
                payload_len = len(tcp.payload) if tcp.payload else 0
                if payload_len > 0:  # Only data packets
                    sent_pkts[tcp.seq] = (payload_len, pkt.time)
            elif ip.src == srvr_ip and ip.dst == clt_ip:
                ack_num = tcp.ack
                for seq_num, (length, send_time) in list(sent_pkts.items()): # Find matching data packet where A = S + L


                    if ack_num == seq_num + length:
                        rtt_val = (pkt.time - send_time) * 1000 # Convert to ms
                        if 0 < rtt_val < 10000: 
                            rtts.append(rtt_val)
                            ack_times.append(pkt.time) # Wall clock time
                        del sent_pkts[seq_num]
                        break


    return rtts, ack_times


# Plotting functions
def plot_throughput(times, throughput, file, title):
    plt.figure()
    plt.plot(times, throughput)
    plt.xlabel('Time (seconds)')
    plt.ylabel('Throughput (bits/second)')
    plt.title(title)
    plt.grid(True)
    plt.savefig(file)
    plt.close()
    print(f"Saved {file}")

def plot_rtt(rtts, ack_times, file):
    if not rtts:
        print("No RTT data to plot")
        return
    start_time = min(ack_times) # Normalize time to start from 0
    normalized_times = [t - start_time for t in ack_times]
    plt.figure()
    plt.plot(normalized_times, rtts, 'ro-', markersize=4)
    plt.xlabel('Time (seconds)')
    plt.ylabel('RTT (ms)')
    plt.title('Round Trip Time (RTT)')
    plt.grid(True)
    plt.savefig(file)
    plt.close()
    print(f"Saved {file}")

# main method
def main():
    parser = argparse.ArgumentParser(description='Analyze network traffic performance')
    parser.add_argument('--file', required=True, help='PCAP file to analyze')  
    parser.add_argument('--client', required=True, help='Client IP address')
    parser.add_argument('--server', required=True, help='Server IP address')
    parser.add_argument('--throughput', action='store_true', help='Calculate throughput')
    parser.add_argument('--down', action='store_true', help='Download throughput')
    parser.add_argument('--up', action='store_true', help='Upload throughput')
    parser.add_argument('--rtt', action='store_true', help='Calculate RTT')
    args = parser.parse_args()
    packets = open_pcap(args.file) # Open and filter packets
    filtered_packets = filter_pcaket(packets, args.client, args.server)
    print(f"Loaded {len(packets)} packets, filtered to {len(filtered_packets)} TCP packets")
    
    if args.throughput:
        if args.down:
            times, thrghput = throughput(filtered_packets, args.client, args.server, "down")
            plot_throughput(times, thrghput, "down_throughput.png", "Download Throughput")
        elif args.up:
            times, thrghput = throughput(filtered_packets, args.client, args.server, "up")
            plot_throughput(times, thrghput, "up_throughput.png", "Upload Throughput")
    elif args.rtt:
        rtts, ack_times = rtt(filtered_packets, args.client, args.server)
        if rtts:
            plot_rtt(rtts, ack_times, "rtt.png")
            print(f"Found {len(rtts)} RTT samples")
        else:
            print("No RTT data found")

if __name__ == '__main__':
    main()

# Client and Server IPs for different captures
# HTTP capture IPs:
# Client ip : 10.194.38.132
# Server ip: 104.208.16.90

# HTTPS capture IPs:  
# Client ip : 10.184.48.75
# Server ip: 20.189.173.10

"""
python3 traffic_analysis.py --file http.pcap --client 10.194.38.132 --server 104.208.16.90 --throughput --down
python3 traffic_analysis.py --file http.pcap --client 10.194.38.132 --server 104.208.16.90 --throughput --up
python3 traffic_analysis.py --file http.pcap --client 10.194.38.132 --server 104.208.16.90 --rtt

python3 traffic_analysis.py --file https.pcap --client 10.184.48.75 --server 20.189.173.10 --throughput --down
python3 traffic_analysis.py --file https.pcap --client 10.184.48.75 --server 20.189.173.10 --throughput --up
python3 traffic_analysis.py --file https.pcap --client 10.184.48.75 --server 20.189.173.10 --rtt
"""

