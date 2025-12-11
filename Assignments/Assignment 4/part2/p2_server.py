import socket
import sys
import time
import struct
import os


# Constants from Part 1
MAX_PAYLOAD = 1200
HEADER_SIZE = 20
DATA_SIZE = MAX_PAYLOAD - HEADER_SIZE  # 1180 bytes
INITIAL_RTO = 0.3
MIN_RTO = 0.1
MAX_RTO = 5
ALPHA = 0.125
BETA = 0.25
EOF_MARKER = b"EOF"


class CongestionControlServer:
    """Extended from Part 1 ReliableServer with congestion control"""

    def __init__(self, server_ip, server_port):
        self.server_ip = server_ip
        self.server_port = server_port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2097152)
        self.socket.bind((server_ip, server_port))

        # Sliding window variables (from Part 1)
        self.send_base = 0
        self.next_seq_num = 0
        self.packets = {}  # seq_num -> (packet, send_time)
        self.dup_ack_count = {}

        # Congestion control (Part 2 addition - TCP Reno)
        self.cwnd = 1.0  # Start with 1 MSS (use float for fractional growth)
        self.ssthresh = 32  # Lower initial threshold for faster convergence
        self.in_slow_start = True
        self.acked_packets_this_rtt = 0  # Track packets, not bytes

        # RTO estimation (from Part 1)
        self.estimated_rtt = INITIAL_RTO
        self.dev_rtt = INITIAL_RTO / 2
        self.rto = INITIAL_RTO
        self.client_addr = None

        # Statistics
        self.total_packets_sent = 0
        self.retransmissions = 0

        print(f"[SERVER] Started on {server_ip}:{server_port}")
        print(f"[SERVER] Congestion Control: cwnd=1 MSS, ssthresh={self.ssthresh}")

    def create_packet(self, seq_num, data):
        """Create packet with header and data (from Part 1)"""
        header = struct.pack('!I', seq_num) + b'\x00' * 16
        return header + data

    def parse_ack(self, packet):
        """Parse ACK packet (from Part 1)"""
        if len(packet) < 4:
            return None
        return struct.unpack('!I', packet[:4])[0]

    def update_rto(self, sample_rtt):
        """Update RTO using TCP's algorithm"""
        if self.estimated_rtt == INITIAL_RTO:
            self.estimated_rtt = sample_rtt
            self.dev_rtt = sample_rtt / 2
        else:
            self.dev_rtt = (1 - BETA) * self.dev_rtt + BETA * abs(self.estimated_rtt - sample_rtt)
            self.estimated_rtt = (1 - ALPHA) * self.estimated_rtt + ALPHA * sample_rtt

        self.rto = self.estimated_rtt + 4 * self.dev_rtt
        self.rto = max(MIN_RTO, min(MAX_RTO, self.rto))

    def on_new_ack(self, acked_packets):
        """Update cwnd on new ACK - TCP Reno congestion control"""
        if self.in_slow_start:
            # Slow Start: Exponential growth (cwnd += 1 for each ACK)
            self.cwnd += acked_packets
            if self.cwnd >= self.ssthresh:
                self.in_slow_start = False
                print(f"[CWND] Exiting slow start at cwnd={self.cwnd:.1f}")
        else:
            # Congestion Avoidance: Linear growth (cwnd += 1/cwnd per ACK)
            # This gives approximately +1 MSS per RTT
            self.cwnd += acked_packets / self.cwnd

    def on_timeout(self):
        """Handle timeout event - conservative approach"""
        print(f"[TIMEOUT] cwnd={self.cwnd:.1f} -> ssthresh={max(int(self.cwnd / 2), 2)}, cwnd=1")
        self.ssthresh = max(int(self.cwnd / 2), 2)
        self.cwnd = 1.0
        self.in_slow_start = True
        # Use 2x backoff instead of 4x
        self.rto = min(self.rto * 2, MAX_RTO)

    def on_fast_retransmit(self):
        """Handle fast retransmit (3 dup ACKs) - TCP Reno"""
        print(f"[FAST_RETX] cwnd={self.cwnd:.1f} -> ssthresh={max(int(self.cwnd / 2), 2)}, cwnd={max(int(self.cwnd / 2), 2) + 3}")
        self.ssthresh = max(int(self.cwnd / 2), 2)
        self.cwnd = float(self.ssthresh + 3)
        self.in_slow_start = False

    def wait_for_client_request(self):
        """Wait for initial client request (from Part 1)"""
        print("[SERVER] Waiting for client request...")
        try:
            data, addr = self.socket.recvfrom(MAX_PAYLOAD)
            self.client_addr = addr
            print(f"[SERVER] Received request from {addr}")
            return True
        except Exception as e:
            print(f"[SERVER] Error: {e}")
            return False

    def send_file(self, filename):
        """Send file with reliability and congestion control"""
        if not os.path.exists(filename):
            print(f"[SERVER] File {filename} not found")
            return False

        with open(filename, 'rb') as f:
            file_data = f.read()

        file_size = len(file_data)

        # Create all packets upfront
        chunks = []
        for i in range(0, file_size, DATA_SIZE):
            chunks.append(file_data[i:i+DATA_SIZE])

        total_packets = len(chunks)
        print(f"[SERVER] File size: {file_size} bytes, Total packets: {total_packets}")

        # Main sending loop
        self.send_base = 0
        self.next_seq_num = 0
        start_time = time.time()
        last_print = start_time
        last_timeout_check = start_time

        # Adaptive timeout based on RTT (increased from 0.0001)
        self.socket.settimeout(0.01)  # 10ms instead of 0.1ms

        ack_batch_limit = 10  # Process multiple ACKs per iteration

        while self.send_base < total_packets:
            current_time = time.time()

            # Send packets within congestion window
            window_size = max(1, int(self.cwnd))  # Ensure at least 1 packet
            while (self.next_seq_num < total_packets and
                   self.next_seq_num < self.send_base + window_size):

                packet = self.create_packet(self.next_seq_num, chunks[self.next_seq_num])
                self.socket.sendto(packet, self.client_addr)
                self.packets[self.next_seq_num] = (packet, current_time)
                self.total_packets_sent += 1
                self.next_seq_num += 1

            # Check timeouts periodically (not every iteration)
            if current_time - last_timeout_check > self.rto / 2:
                timeout_occurred = False
                for seq_num in list(self.packets.keys()):
                    packet, send_time = self.packets[seq_num]
                    if current_time - send_time > self.rto:
                        self.socket.sendto(packet, self.client_addr)
                        self.packets[seq_num] = (packet, current_time)
                        self.retransmissions += 1
                        if not timeout_occurred:
                            self.on_timeout()
                            timeout_occurred = True
                        break  # Only handle one timeout per check
                last_timeout_check = current_time

            # Receive ACKs in batch
            acks_processed = 0
            while acks_processed < ack_batch_limit:
                try:
                    ack_packet, _ = self.socket.recvfrom(1024)
                    ack_seq = self.parse_ack(ack_packet)

                    if ack_seq is not None:
                        if ack_seq > self.send_base:
                            # New ACK
                            acked_packets = ack_seq - self.send_base

                            # Update RTO based on first packet in window
                            if self.send_base in self.packets:
                                _, send_time = self.packets[self.send_base]
                                sample_rtt = current_time - send_time
                                if sample_rtt > 0 and sample_rtt < self.rto * 2:  # Sanity check
                                    self.update_rto(sample_rtt)

                            # Remove acked packets
                            for seq in range(self.send_base, ack_seq):
                                if seq in self.packets:
                                    del self.packets[seq]
                                if seq in self.dup_ack_count:
                                    del self.dup_ack_count[seq]

                            self.send_base = ack_seq
                            self.on_new_ack(acked_packets)

                        elif ack_seq == self.send_base and self.send_base < total_packets:
                            # Duplicate ACK
                            self.dup_ack_count[ack_seq] = self.dup_ack_count.get(ack_seq, 0) + 1

                            if self.dup_ack_count[ack_seq] == 3:
                                # Fast retransmit
                                if self.send_base in self.packets:
                                    packet, _ = self.packets[self.send_base]
                                    self.socket.sendto(packet, self.client_addr)
                                    self.packets[self.send_base] = (packet, current_time)
                                    self.retransmissions += 1
                                    self.on_fast_retransmit()

                    acks_processed += 1

                except socket.timeout:
                    break

            # Progress reporting
            if current_time - last_print > 1.0:
                elapsed = current_time - start_time
                progress = (self.send_base / total_packets) * 100
                bytes_sent = self.send_base * DATA_SIZE
                throughput = bytes_sent / elapsed / 1024 / 1024
                print(f"[SERVER] {progress:.1f}% | {throughput:.2f} MB/s | cwnd={self.cwnd:.1f} | rto={self.rto:.3f}s")
                last_print = current_time

        total_time = time.time() - start_time
        throughput = file_size / total_time / 1024 / 1024
        print(f"[SERVER] Complete: {total_time:.2f}s, {throughput:.2f} MB/s")
        print(f"[SERVER] Packets sent: {self.total_packets_sent}, Retransmissions: {self.retransmissions}")

        # Send EOF
        eof_packet = self.create_packet(total_packets, EOF_MARKER)
        for _ in range(20):
            self.socket.sendto(eof_packet, self.client_addr)
            time.sleep(0.01)

        return True

    def close(self):
        self.socket.close()


def main():
    if len(sys.argv) != 3:
        print("Usage: python3 p2_server.py <SERVER_IP> <SERVER_PORT>")
        sys.exit(1)

    server = CongestionControlServer(sys.argv[1], int(sys.argv[2]))

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