#!/usr/bin/env python3
import socket
import threading
import json
import time
import os
import queue
import logging
import sys

class FCFSServer:
    def __init__(self, config_file='config.json'):
        with open(config_file, 'r') as f:
            self.config = json.load(f)
        
        self.server_ip = self.config['server_ip']
        self.port = self.config['port']
        self.words_file = 'words.txt'
        self.words = self.load_words()
        self.request_queue = queue.Queue()
        self.client_connections = {}
        self.connection_lock = threading.Lock()
        self.running = False
        
        os.makedirs('logs', exist_ok=True)
        log_file = 'logs/server.log'
        if os.path.exists(log_file):
            os.remove(log_file)
            
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)])
        self.logger = logging.getLogger(__name__)

    def load_words(self):
        try:
            with open(self.words_file, 'r') as f:
                return f.read().strip().split(',')
        except FileNotFoundError:
            self.logger.error(f"Words file not found: {self.words_file}")
            return []

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
        client_id = f"{client_addr[0]}:{client_addr[1]}"
        self.logger.info(f"Client {client_id} connected.")
        buffer = b''
        
        try:
            while self.running:
                data = client_socket.recv(1024)
                if not data:
                    break
                
                buffer += data
                
                # Process all complete requests in the buffer
                while b'\n' in buffer:
                    request_line, buffer = buffer.split(b'\n', 1)
                    request = request_line.decode('utf-8').strip()
                    if ',' in request:
                        try:
                            p, k = map(int, request.split(','))
                            self.request_queue.put({'client_socket': client_socket, 'p': p, 'k': k, 'client_id': client_id})
                        except ValueError:
                            self.logger.warning(f"Invalid request from {client_id}: {request}")
        except ConnectionResetError:
            self.logger.info(f"Client {client_id} reset connection.")
        except Exception as e:
            self.logger.error(f"Error with client {client_id}: {e}")
        finally:
            client_socket.close()
            self.logger.info(f"Client {client_id} disconnected.")

    def request_processor(self):
        while self.running or not self.request_queue.empty():
            try:
                item = self.request_queue.get(timeout=0.1)
                response = self.process_request(item['p'], item['k'])
                try:
                    item['client_socket'].sendall(response.encode('utf-8'))
                    # This tiny delay makes the simulation more stable
                    time.sleep(0.0015) 
                except (BrokenPipeError, ConnectionResetError):
                    self.logger.warning(f"Client {item['client_id']} disconnected before response could be sent.")
                self.request_queue.task_done()
            except queue.Empty:
                continue

    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.server_ip, self.port))
        self.server_socket.listen(25) # Increased backlog for bursts
        self.running = True
        self.logger.info(f"Server listening on {self.server_ip}:{self.port}")

        processor_thread = threading.Thread(target=self.request_processor, daemon=True)
        processor_thread.start()

        try:
            while self.running:
                client_socket, client_addr = self.server_socket.accept()
                client_thread = threading.Thread(target=self.handle_client_connection, args=(client_socket, client_addr), daemon=True)
                client_thread.start()
        except KeyboardInterrupt:
            self.logger.info("Shutdown signal received.")
        finally:
            self.stop()
            
    def stop(self):
        self.running = False
        self.server_socket.close()
        self.logger.info("Server stopped.")

def main():
    server = FCFSServer()
    try:
        server.start()
    except Exception as e:
        print(f"Failed to start server: {e}")
    finally:
        server.stop()

if __name__ == '__main__':
    main()
