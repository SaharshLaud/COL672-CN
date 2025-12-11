import socket
import sys
import struct
import time

# Constants
MAX_PAYLOAD = 1200
HEADER_SIZE = 20
DATA_SIZE = MAX_PAYLOAD - HEADER_SIZE
MAX_RETRIES = 5
RETRY_TIMEOUT = 2.0
EOF_MARKER = b"EOF"

class ReliableClient:
    def __init__(self, server_ip, server_port):
        self.server_ip = server_ip
        self.server_port = int(server_port)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(0.003)  # 1ms - very responsive
        
        self.expected_seq = 0
        self.received_data = {}
        self.output_file = 'received_data.txt'
        self.eof_seq = None
        
        print(f"[CLIENT] Initialized for {server_ip}:{server_port}")
    
    def parse_packet(self, packet):
        """Parse packet"""
        if len(packet) < HEADER_SIZE:
            return None, None
        seq_num = struct.unpack('!I', packet[:4])[0]
        data = packet[HEADER_SIZE:]
        return seq_num, data
    
    def create_ack(self, cum_ack, sack_blocks=[]):
        """Create ACK packet"""
        ack_packet = struct.pack('!I', cum_ack)
        
        for i in range(min(2, len(sack_blocks))):
            left, right = sack_blocks[i]
            ack_packet += struct.pack('!II', left, right)
        
        while len(ack_packet) < HEADER_SIZE:
            ack_packet += b'\x00'
        
        return ack_packet
    
    def send_request(self):
        """Send initial request"""
        request = b'\x01'
        
        for attempt in range(MAX_RETRIES):
            print(f"[CLIENT] Request attempt {attempt + 1}")
            self.socket.sendto(request, (self.server_ip, self.server_port))
            
            start = time.time()
            while time.time() - start < RETRY_TIMEOUT:
                try:
                    data, _ = self.socket.recvfrom(MAX_PAYLOAD)
                    seq_num, payload = self.parse_packet(data)
                    if seq_num is not None:
                        print(f"[CLIENT] Connected (first seq: {seq_num})")
                        if payload == EOF_MARKER:
                            self.eof_seq = seq_num
                        else:
                            self.received_data[seq_num] = payload
                        return True
                except socket.timeout:
                    continue
                except:
                    continue
        
        return False
    
    def write_in_order_data(self):
        """Write in-order data to file"""
        wrote_eof = False
        with open(self.output_file, 'ab') as f:
            while self.expected_seq in self.received_data:
                data = self.received_data.pop(self.expected_seq)
                
                if data == EOF_MARKER:
                    self.eof_seq = self.expected_seq
                    wrote_eof = True
                    break
                
                f.write(data)
                self.expected_seq += len(data)
        
        return wrote_eof
    
    def generate_sack_blocks(self):
        """Generate SACK blocks"""
        if not self.received_data:
            return []
        
        sorted_seqs = sorted(self.received_data.keys())
        blocks = []
        start = sorted_seqs[0]
        end = start + len(self.received_data[start])
        
        for seq in sorted_seqs[1:]:
            if seq == end:
                end = seq + len(self.received_data[seq])
            else:
                blocks.append((start, end))
                start = seq
                end = seq + len(self.received_data[seq])
        
        blocks.append((start, end))
        return blocks[:2]
    
    def receive_file(self):
        """Receive file"""
        print("[CLIENT] Receiving file...")
        
        with open(self.output_file, 'wb') as f:
            pass
        
        start_time = time.time()
        last_print = start_time
        last_ack_time = 0
        packets_received = 0
        no_data_count = 0
        
        while self.eof_seq is None and no_data_count < 500:
            current_time = time.time()
            packets_this_round = 0
            
            # Receive multiple packets per iteration
            while packets_this_round < 60:
                try:
                    packet, _ = self.socket.recvfrom(MAX_PAYLOAD)
                    seq_num, data = self.parse_packet(packet)
                    packets_this_round += 1
                    
                    if seq_num is not None:
                        packets_received += 1
                        no_data_count = 0
                        
                        if data == EOF_MARKER:
                            self.eof_seq = seq_num
                            self.write_in_order_data()
                            break
                        
                        if seq_num >= self.expected_seq and seq_num not in self.received_data:
                            self.received_data[seq_num] = data
                        
                        self.write_in_order_data()
                
                except socket.timeout:
                    break
                except:
                    break
            
            # Send ACK (not too frequently, but not too slow)
            if current_time - last_ack_time > 0.003 or packets_this_round > 0:  # 1ms or after receiving packets
                sack_blocks = self.generate_sack_blocks()
                ack = self.create_ack(self.expected_seq, sack_blocks)
                self.socket.sendto(ack, (self.server_ip, self.server_port))
                last_ack_time = current_time
            
            if packets_this_round == 0:
                no_data_count += 1
            
            # Progress
            if current_time - last_print > 1.0:
                elapsed = current_time - start_time
                throughput = self.expected_seq / elapsed / 1024 / 1024
                print(f"[CLIENT] {self.expected_seq/1024/1024:.1f} MB received | {throughput:.2f} MB/s | Buffer: {len(self.received_data)}")
                last_print = current_time
        
        # Send final ACKs
        if self.eof_seq is not None:
            ack = self.create_ack(self.expected_seq)
            for _ in range(5):
                self.socket.sendto(ack, (self.server_ip, self.server_port))
        
        total_time = time.time() - start_time
        throughput = self.expected_seq / total_time / 1024 / 1024
        print(f"[CLIENT] Complete in {total_time:.2f}s ({throughput:.2f} MB/s)")
        return True
    
    def close(self):
        self.socket.close()

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 p1_client.py <SERVER_IP> <SERVER_PORT>")
        sys.exit(1)
    
    client = ReliableClient(sys.argv[1], sys.argv[2])
    
    try:
        if client.send_request():
            client.receive_file()
    except KeyboardInterrupt:
        print("\n[CLIENT] Interrupted")
    except Exception as e:
        print(f"[CLIENT] Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()

if __name__ == "__main__":
    main()