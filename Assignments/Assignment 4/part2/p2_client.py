import socket
import argparse
import struct

# Constants
MAX_PAYLOAD = 1200
HEADER_SIZE = 20
MAX_DATA = MAX_PAYLOAD - HEADER_SIZE
EOF_MARKER = b"EOF"

def parse_packet(packet):
    """
    Parse packet to extract sequence number and data
    Header structure:
    - First 4 bytes: sequence number
    - Next 16 bytes: reserved
    - Rest: data
    """
    if len(packet) < HEADER_SIZE:
        return -1, b""
    
    seq_num = struct.unpack('!I', packet[:4])[0]
    data = packet[HEADER_SIZE:]
    return seq_num, data

def create_ack(seq_num):
    """Create ACK packet with next expected sequence number"""
    return struct.pack('!I', seq_num) + b'\x00' * 16

def receive_file(server_ip, server_port, pref_filename):
    """
    Receive file from server and save with prefix
    """
    # Initialize UDP socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_socket.settimeout(5.0)
    server_address = (server_ip, server_port)
    
    # Send connection request
    max_retries = 5
    for attempt in range(max_retries):
        try:
            print(f"Attempting connection (attempt {attempt + 1}/{max_retries})")
            client_socket.sendto(b"START", server_address)
            # Wait for first packet
            packet, _ = client_socket.recvfrom(MAX_PAYLOAD + 100)
            break
        except socket.timeout:
            if attempt == max_retries - 1:
                print("Failed to connect to server")
                return
            continue
    
    # Open output file
    output_file = f"{pref_filename}received_data.txt"
    with open(output_file, 'wb') as file:
        expected_seq_num = 0
        out_of_order_packets = {}  # Buffer for out-of-order packets
        
        while True:
            try:
                seq_num, data = parse_packet(packet)
                
                if seq_num == expected_seq_num:
                    # In-order packet
                    if data == EOF_MARKER:
                        print("Received EOF")
                        # Send final ACK
                        ack = create_ack(expected_seq_num + 1)
                        client_socket.sendto(ack, server_address)
                        break
                    
                    # Write data to file
                    file.write(data)
                    print(f"Received packet {seq_num}")
                    expected_seq_num += 1
                    
                    # Check if we have buffered packets
                    while expected_seq_num in out_of_order_packets:
                        buffered_data = out_of_order_packets[expected_seq_num]
                        if buffered_data == EOF_MARKER:
                            print("Received EOF from buffer")
                            ack = create_ack(expected_seq_num + 1)
                            client_socket.sendto(ack, server_address)
                            expected_seq_num += 1
                            break
                        file.write(buffered_data)
                        print(f"Wrote buffered packet {expected_seq_num}")
                        del out_of_order_packets[expected_seq_num]
                        expected_seq_num += 1
                    
                    # Send cumulative ACK
                    ack = create_ack(expected_seq_num)
                    client_socket.sendto(ack, server_address)
                    
                elif seq_num > expected_seq_num:
                    # Out-of-order packet - buffer it
                    print(f"Buffering out-of-order packet {seq_num}")
                    out_of_order_packets[seq_num] = data
                    
                    # Send duplicate ACK for expected packet
                    ack = create_ack(expected_seq_num)
                    client_socket.sendto(ack, server_address)
                
                else:
                    # Old/duplicate packet - send ACK again
                    print(f"Received duplicate packet {seq_num}")
                    ack = create_ack(expected_seq_num)
                    client_socket.sendto(ack, server_address)
                
                # Receive next packet
                packet, _ = client_socket.recvfrom(MAX_PAYLOAD + 100)
                
            except socket.timeout:
                print("Timeout waiting for data")
                break
    
    print(f"File saved as {output_file}")
    client_socket.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='UDP file transfer client')
    parser.add_argument('server_ip', help='Server IP address')
    parser.add_argument('server_port', type=int, help='Server port number')
    parser.add_argument('pref_filename', help='Prefix for output filename')
    args = parser.parse_args()
    
    receive_file(args.server_ip, args.server_port, args.pref_filename)