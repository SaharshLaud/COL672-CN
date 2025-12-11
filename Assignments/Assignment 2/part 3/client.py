#!/usr/bin/env python3
import socket
import json
import time
import argparse
import os
import sys
import traceback

class WordCountingClient:
    def __init__(self, config_file='config.json', batch_size=1, client_id='client'):
        try:
            with open(config_file, 'r') as f:
                self.config = json.load(f)
        except Exception as e:
            print(f"ERROR: Failed to load config file {config_file}: {e}")
            sys.exit(1)
        
        self.server_ip = self.config['server_ip']
        self.port = self.config['port']
        self.k = self.config['k']
        self.p = self.config['p']
        
        self.batch_size = batch_size
        self.client_id = client_id
        
        os.makedirs('logs', exist_ok=True)
        self.word_counts = {}
        
        print(f"Client {self.client_id} initialized: batch_size={self.batch_size}, server={self.server_ip}:{self.port}")

    def count_words(self, words_str):
        if words_str and 'EOF' not in words_str:
            words = [word.strip() for word in words_str.split(',') if word.strip()]
            for word in words:
                self.word_counts[word] = self.word_counts.get(word, 0) + 1
        elif 'EOF' in words_str:
            # Handle responses that contain both words and the EOF token
            parts = words_str.split(',EOF')
            if parts[0]:
                words = [word.strip() for word in parts[0].split(',') if word.strip()]
                for word in words:
                    self.word_counts[word] = self.word_counts.get(word, 0) + 1

    def download_file(self):
        start_time = time.time()
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30.0) # More reasonable timeout
            sock.connect((self.server_ip, self.port))
            print(f"[{self.client_id}] Connected successfully.")
            
            current_offset = self.p
            eof_received = False
            
            # Create a buffer to handle TCP stream data
            buffer = b''

            while not eof_received:
                # GREEDY CLIENT LOGIC 
                if self.batch_size > 1:
                    # 1. Send a burst of 'c' requests without waiting
                    requests_to_send = self.batch_size
                    for i in range(requests_to_send):
                        request = f"{current_offset + i * self.k},{self.k}\n"
                        sock.sendall(request.encode('utf-8'))
                    
                    # 2. Receive a burst of 'c' responses
                    for i in range(requests_to_send):
                        while b'\n' not in buffer:
                            data = sock.recv(1024)
                            if not data:
                                eof_received = True
                                break
                            buffer += data
                        
                        if eof_received:
                            break

                        response_line, buffer = buffer.split(b'\n', 1)
                        response = response_line.decode('utf-8')
                        
                        if 'EOF' in response:
                            self.count_words(response)
                            eof_received = True
                            break 
                        else:
                            self.count_words(response)

                    current_offset += requests_to_send * self.k

                # NORMAL CLIENT LOGIC
                else:
                    request = f"{current_offset},{self.k}\n"
                    sock.sendall(request.encode('utf-8'))
                    
                    while b'\n' not in buffer:
                        data = sock.recv(1024)
                        if not data:
                            eof_received = True
                            break
                        buffer += data

                    if eof_received:
                        break

                    response_line, buffer = buffer.split(b'\n', 1)
                    response = response_line.decode('utf-8')

                    if 'EOF' in response:
                        self.count_words(response)
                        eof_received = True
                    else:
                        self.count_words(response)
                        current_offset += self.k

            sock.close()

        except Exception as e:
            print(f"[{self.client_id}] CRITICAL ERROR: {e}")
            traceback.print_exc()
            sys.exit(1) # Ensure the runner knows this client failed

        end_time = time.time()
        total_time = (end_time - start_time) * 1000

        completion_file = f'logs/{self.client_id}_completion.txt'
        with open(completion_file, 'w') as f:
            f.write(f"{total_time:.2f}\n")
        
        return total_time

def main():
    parser = argparse.ArgumentParser(description='Word Counting Client')
    parser.add_argument('--batch-size', type=int, default=1, help='Number of requests to send back-to-back (c value)')
    parser.add_argument('--client-id', type=str, default='client', help='Unique client identifier')
    args = parser.parse_args()

    client = WordCountingClient(batch_size=args.batch_size, client_id=args.client_id)
    completion_time = client.download_file()

    if completion_time:
        print(f"Client {args.client_id} completed successfully in {completion_time:.2f}ms")
    else:
        print(f"Client {args.client_id} failed.")
        sys.exit(1)

if __name__ == '__main__':
    main()
