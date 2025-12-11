# client.py
#!/usr/bin/env python3

import socket
import json
import time
import argparse

# Connect to the server and download the word file
def download_file(config, client_id, quiet):
    server_ip = config['server_ip']
    port = config['port']
    p = config['p']
    k = config.get('k_override', config['k']) # Allow k to be overridden

    start_time = time.time()
    
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((server_ip, port))
        
        all_words = []
        offset = p
        
        while True:
            request = f"{offset},{k}\n"
            client_socket.send(request.encode())
            
            response = client_socket.recv(4096).decode().strip()
            
            if not response:
                break
            
            # Split response into words
            words_received = response.split(',')
            
            if "EOF" in words_received:
                words_received.remove("EOF")
                if words_received:
                    all_words.extend(w for w in words_received if w)
                break
            
            all_words.extend(w for w in words_received if w)
            offset += k
            
    except Exception as e:
        if not quiet:
            print(f"[{client_id}] Error: {e}")
        return None
    finally:
        client_socket.close()

    end_time = time.time()
    completion_time = end_time - start_time
    
    if not quiet:
        print(f"[{client_id}] Download complete in {completion_time:.3f} seconds.")
        
        # Manual word frequency count using a dictionary
        word_counts = {}
        for word in all_words:
            word_counts[word] = word_counts.get(word, 0) + 1
            
        # Print sorted results
        for word, count in sorted(word_counts.items()):
            print(f"{word}, {count}")
            
    return completion_time

# Parses arguments and runs the client

def main():
    parser = argparse.ArgumentParser(description='Word Client for Part 2')
    parser.add_argument('client_id', nargs='?', default='client', help='An identifier for the client')
    parser.add_argument('--config', default='config.json', help='Path to the configuration file')
    parser.add_argument('--quiet', action='store_true', help='Suppress non-essential output')
    parser.add_argument('--k', type=int, help='Override the value of k from the config file')
    
    args = parser.parse_args()
    
    with open(args.config, 'r') as f:
        config = json.load(f)

    if args.k:
        config['k_override'] = args.k
        
    download_file(config, args.client_id, args.quiet)

if __name__ == "__main__":
    main()