import socket
import sys
import time
import struct
import os

# Constants
MAX_PAYLOAD = 1200
HEADER_SIZE = 20
DATA_SIZE = MAX_PAYLOAD - HEADER_SIZE  # 1180 bytes
INITIAL_RTO = 0.5
MIN_RTO = 0.1
MAX_RTO = 5
ALPHA = 0.125
BETA = 0.25
EOF_MARKER = b"EOF"

class ReliableServer:
    def __init__(self, server_ip, server_port, sws):
        self.server_ip = server_ip
        self.server_port = server_port
        self.sws = sws  # Sender Window Size in bytes
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((server_ip, server_port))
        self.socket.settimeout(0.003)  # 1ms - very responsive
        
        # Sliding window variables
        self.send_base = 0
        self.next_seq_num = 0
        self.packets = {}  # seq_num -> (data, send_time, packet_bytes)
        self.dup_ack_count = 0
        self.last_ack = 0
        
        # RTO estimation
        self.estimated_rtt = None
        self.dev_rtt = 0
        self.rto = INITIAL_RTO
        
        self.client_addr = None
        self.total_packets_sent = 0
        self.retransmissions = 0
        
        print(f"[SERVER] Started on {server_ip}:{server_port}, SWS={sws} bytes")
    
    def create_packet(self, seq_num, data):
        """Create packet with header and data"""
        header = struct.pack('!I', seq_num) + b'\x00' * 16
        return header + data
    
    def parse_ack(self, packet):
        """Parse ACK packet"""
        if len(packet) < 4:
            return None, []
        
        cum_ack = struct.unpack('!I', packet[:4])[0]
        
        # Parse SACK blocks
        sack_blocks = []
        if len(packet) >= 20:
            try:
                for i in range(2):
                    offset = 4 + i * 8
                    if offset + 8 <= len(packet):
                        left = struct.unpack('!I', packet[offset:offset+4])[0]
                        right = struct.unpack('!I', packet[offset+4:offset+8])[0]
                        if left > 0 and right > 0 and right > left:
                            sack_blocks.append((left, right))
            except:
                pass
        
        return cum_ack, sack_blocks
    
    def update_rto(self, sample_rtt):
        """Update RTO"""
        if self.estimated_rtt is None:
            self.estimated_rtt = sample_rtt
            self.dev_rtt = sample_rtt / 2
        else:
            self.dev_rtt = (1 - BETA) * self.dev_rtt + BETA * abs(sample_rtt - self.estimated_rtt)
            self.estimated_rtt = (1 - ALPHA) * self.estimated_rtt + ALPHA * sample_rtt
        
        self.rto = self.estimated_rtt + 6 * self.dev_rtt
        self.rto = max(MIN_RTO, min(MAX_RTO, self.rto))
    
    def wait_for_client_request(self):
        """Wait for initial client request"""
        print("[SERVER] Waiting for client request...")
        self.socket.settimeout(None)  # Blocking wait
        try:
            data, addr = self.socket.recvfrom(MAX_PAYLOAD)
            self.client_addr = addr
            print(f"[SERVER] Received request from {addr}")
            self.socket.settimeout(0.003)  # Back to non-blocking
            return True
        except Exception as e:
            print(f"[SERVER] Error: {e}")
            return False
    
    def send_file(self, filename):
        """Send file with reliability"""
        if not os.path.exists(filename):
            print(f"[SERVER] File {filename} not found")
            return False
        
        with open(filename, 'rb') as f:
            file_data = f.read()
        
        file_size = len(file_data)
        print(f"[SERVER] File size: {file_size} bytes")
        
        # Create all packets upfront
        chunks = []
        seq_nums = []
        current_seq = 0
        
        for i in range(0, file_size, DATA_SIZE):
            chunk = file_data[i:i+DATA_SIZE]
            chunks.append(chunk)
            seq_nums.append(current_seq)
            current_seq += len(chunk)
        
        total_packets = len(chunks)
        print(f"[SERVER] Total packets: {total_packets}, Window can hold ~{self.sws // DATA_SIZE} packets")
        
        # Main sending loop
        self.send_base = 0
        self.next_seq_num = 0
        start_time = time.time()
        last_print = start_time
        
        while self.send_base < file_size:
            current_time = time.time()
            
            # Send packets to fill window
            packets_sent_this_round = 0
            while (self.next_seq_num < file_size and 
                   self.next_seq_num - self.send_base < self.sws and
                   packets_sent_this_round < 60):  # Burst limit
                
                # Find the chunk for this sequence number
                chunk_idx = None
                for idx, seq in enumerate(seq_nums):
                    if seq == self.next_seq_num:
                        chunk_idx = idx
                        break
                
                if chunk_idx is not None and chunk_idx < len(chunks):
                    seq = seq_nums[chunk_idx]
                    if seq not in self.packets:  # Don't resend if already in flight
                        packet_data = chunks[chunk_idx]
                        packet = self.create_packet(seq, packet_data)
                        
                        self.socket.sendto(packet, self.client_addr)
                        self.packets[seq] = (packet_data, current_time, packet)
                        self.total_packets_sent += 1
                        packets_sent_this_round += 1
                        
                        self.next_seq_num = seq + len(packet_data)
                else:
                    break
            
            # Process ACKs (multiple per iteration)
            acks_processed = 0
            while acks_processed < 50:  # Process up to 50 ACKs per round
                try:
                    ack_packet, _ = self.socket.recvfrom(MAX_PAYLOAD)
                    cum_ack, sack_blocks = self.parse_ack(ack_packet)
                    acks_processed += 1
                    
                    if cum_ack is not None and cum_ack > self.send_base:
                        # New ACK - update window
                        if self.send_base in self.packets:
                            _, send_time, _ = self.packets[self.send_base]
                            sample_rtt = current_time - send_time
                            if sample_rtt > 0:
                                self.update_rto(sample_rtt)
                        
                        # Remove acknowledged packets
                        acked_seqs = [s for s in list(self.packets.keys()) if s < cum_ack]
                        for s in acked_seqs:
                            del self.packets[s]
                        
                        self.send_base = cum_ack
                        self.dup_ack_count = 0
                        self.last_ack = cum_ack
                        
                    elif cum_ack == self.send_base and self.send_base < file_size:
                        # Duplicate ACK
                        self.dup_ack_count += 1
                        
                        if self.dup_ack_count == 3:
                            # Fast retransmit
                            if self.send_base in self.packets:
                                _, _, packet = self.packets[self.send_base]
                                self.socket.sendto(packet, self.client_addr)
                                self.packets[self.send_base] = (self.packets[self.send_base][0], current_time, packet)
                                self.retransmissions += 1
                                self.dup_ack_count = 0
                
                except socket.timeout:
                    break
                except Exception:
                    break
            
            # Check for timeouts (only oldest packet)
            if len(self.packets) > 0:
                oldest_seq = min(self.packets.keys())
                _, send_time, packet = self.packets[oldest_seq]
                if current_time - send_time > self.rto:
                    self.socket.sendto(packet, self.client_addr)
                    self.packets[oldest_seq] = (self.packets[oldest_seq][0], current_time, packet)
                    self.retransmissions += 1
            
            # Progress
            if current_time - last_print > 1.0:
                elapsed = current_time - start_time
                progress = (self.send_base / file_size) * 100
                throughput = self.send_base / elapsed / 1024 / 1024  # MB/s
                print(f"[SERVER] {progress:.1f}% | {throughput:.2f} MB/s | Window: {len(self.packets)} pkts | RTO: {self.rto:.3f}s")
                last_print = current_time
        
        total_time = time.time() - start_time
        throughput = file_size / total_time / 1024 / 1024
        print(f"[SERVER] Transfer complete in {total_time:.2f}s ({throughput:.2f} MB/s)")
        print(f"[SERVER] Packets sent: {self.total_packets_sent}, Retransmissions: {self.retransmissions}")
        
        # Send EOF
        print("[SERVER] Sending EOF")
        eof_packet = self.create_packet(file_size, EOF_MARKER)
        for _ in range(10):
            self.socket.sendto(eof_packet, self.client_addr)
            time.sleep(0.02)
        
        return True
    
    def close(self):
        self.socket.close()

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 p1_server.py <SERVER_IP> <SERVER_PORT> <SWS>")
        sys.exit(1)
    
    server = ReliableServer(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
    
    try:
        if server.wait_for_client_request():
            server.send_file('data.txt')
    except KeyboardInterrupt:
        print("\n[SERVER] Interrupted")
    except Exception as e:
        print(f"[SERVER] Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        server.close()

if __name__ == "__main__":
    main()