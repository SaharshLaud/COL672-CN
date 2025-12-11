# server.py
#!/usr/bin/env python3

import socket
import json
import select

# """Load from word.txt file into a list
def load_words(filename):
    with open(filename, 'r') as f:
        content = f.read().strip()
    return content.split(',')

# Parse a 'p,k' request and sends back the corresponding words
def process_request(client_socket, request, words):
    try:
        parts = request.split(',')
        if len(parts) != 2:
            response = "Error: Invalid request format\n"
        else:
            p = int(parts[0])
            k = int(parts[1])
            
            if p >= len(words):
                response = "EOF\n"
            else:
                end_idx = p + k
                requested_words = words[p:end_idx]
                
                if end_idx >= len(words):
                    requested_words.append("EOF")
                
                response = ",".join(requested_words) + "\n"
        
        client_socket.send(response.encode())
    except (ValueError, IndexError) as e:
        client_socket.send(f"Error: {e}\n".encode())
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Main server loop to listen for and handle client connections
def run_server(config):
    words = load_words(config['filename'])
    print(f"Server loaded {len(words)} words.")

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((config['server_ip'], config['port']))
    server_socket.listen(15)
    
    print(f"Server listening on {config['server_ip']}:{config['port']}")
    
    client_sockets = [server_socket]
    request_queue = []

    try:
        while True:
            readable_sockets, _, _ = select.select(client_sockets, [], [])
            
            for sock in readable_sockets:
                if sock == server_socket:
                    # A new client is connecting
                    client_socket, addr = server_socket.accept()
                    client_sockets.append(client_socket)
                    print(f"Accepted connection from {addr}")
                else:
                    # Data received from an existing client
                    try:
                        data = sock.recv(1024).decode().strip()
                        if data:
                            request_queue.append((sock, data))
                        else:
                            # Client disconnected
                            print(f"Client {sock.getpeername()} disconnected.")
                            sock.close()
                            client_sockets.remove(sock)
                    except ConnectionResetError:
                        print(f"Client {sock.getpeername()} reset the connection.")
                        sock.close()
                        if sock in client_sockets:
                            client_sockets.remove(sock)

            # Process all requests in the queue (FCFS)
            processed_in_loop = 0
            while request_queue and processed_in_loop == 0:
                client_sock, req = request_queue.pop(0)
                if client_sock.fileno() != -1: # Check if socket is still valid
                    process_request(client_sock, req, words)
                    processed_in_loop = 1 # We process one request at a time

    except KeyboardInterrupt:
        print("\nServer shutting down.")
    finally:
        for sock in client_sockets:
            sock.close()

if __name__ == "__main__":
    with open('config.json', 'r') as f:
        config_data = json.load(f)
    run_server(config_data)