#!/usr/bin/env python3
import socket
import threading
import json
import time
import os
import queue
import logging
import sys
import random
from collections import deque

class RoundRobinServer:
    def __init__(self, config_file='config.json'):
        with open(config_file, 'r') as f:
            self.config = json.load(f)
        
        self.server_ip = self.config['server_ip']
        self.port = self.config['port']
        self.words = self.load_words()
        
        # Data structures for Round-Robin Scheduling
        self.client_queues = {}
        self.client_order = deque()
        self.lock = threading.Lock()
        self.running = False
        
        os.makedirs('logs', exist_ok=True)
        log_file = 'logs/server.log'
        if os.path.exists(log_file):
            os.remove(log_file)
            
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - RR-SERVER - %(message)s', handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)])
        self.logger = logging.getLogger(__name__)

    def load_words(self):
        with open('words.txt', 'r') as f:
            return f.read().strip().split(',')

    def process_request(self, p, k):
        total_words = len(self.words)
        if p >= total_words:
            return "EOF\n"
        
        end_idx = min(p + k, total_words)
        requested_words = self.words[p:end_idx]
        
        if end_idx >= total_words:
            return ','.join(requested_words) + ',EOF\n' if requested_words else "EOF\n"
        else:
            return ','.join(requested_words) + '\n'

    def handle_client_connection(self, client_socket, client_addr):
        client_id = client_socket.getpeername()
        self.logger.info(f"Client {client_id} connected and added to RR schedule.")
        
        with self.lock:
            self.client_queues[client_id] = queue.Queue()
            self.client_order.append(client_id)
            
        buffer = b''
        try:
            while self.running:
                data = client_socket.recv(4096)
                if not data: break
                buffer += data
                while b'\n' in buffer:
                    request_line, buffer = buffer.split(b'\n', 1)
                    request = request_line.decode('utf-8').strip()
                    if ',' in request and client_id in self.client_queues:
                        p, k = map(int, request.split(','))
                        self.client_queues[client_id].put({'socket': client_socket, 'p': p, 'k': k})
        except (ConnectionResetError, BrokenPipeError, KeyError):
            self.logger.warning(f"Client {client_id} disconnected.")
        finally:
            with self.lock:
                if client_id in self.client_queues: del self.client_queues[client_id]
                if client_id in self.client_order: self.client_order.remove(client_id)
            client_socket.close()

    def request_processor(self):
        while self.running:
            client_to_serve = None
            with self.lock:
                if self.client_order:
                    client_to_serve = self.client_order.popleft()
                    self.client_order.append(client_to_serve)

            if client_to_serve and self.client_queues.get(client_to_serve) and not self.client_queues[client_to_serve].empty():
                try:
                    item = self.client_queues[client_to_serve].get_nowait()
                    response = self.process_request(item['p'], item['k'])
                    
                    # Base delay from Part 3 + random jitter
                    base_delay = 0.015  # 15ms, same as Part 3
                    jitter = random.uniform(0, 0.02) # 0-20ms random jitter
                    time.sleep(base_delay + jitter)
                    
                    item['socket'].sendall(response.encode('utf-8'))
                except (queue.Empty, KeyError, BrokenPipeError, ConnectionResetError):
                    continue
            else:
                time.sleep(0.001)

    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.server_ip, self.port))
        self.server_socket.listen(128)
        self.running = True
        self.logger.info(f"Round-Robin Server on {self.server_ip}:{self.port}")

        threading.Thread(target=self.request_processor, daemon=True).start()

        try:
            while self.running:
                client_socket, client_addr = self.server_socket.accept()
                threading.Thread(target=self.handle_client_connection, args=(client_socket, client_addr), daemon=True).start()
        except KeyboardInterrupt:
            self.logger.info("Server shutting down.")
        finally:
            self.stop()

    def stop(self):
        self.running = False
        self.server_socket.close()

if __name__ == '__main__':
    RoundRobinServer().start()
