#!/usr/bin/env python3

import json
import time
import csv
from topology import create_network

class Part2Runner:
    def __init__(self, config_file='config.json'):
        with open(config_file, 'r') as f:
            self.config = json.load(f)
        
        self.server_ip = self.config['server_ip']
        self.port = self.config['port']
        self.num_iterations = self.config.get('num_iterations', 5)
        
        # Client numbers to test
        self.client_numbers = [1, 5, 9, 13, 17, 21, 25, 29, 32]
    
    # Run experiment with specified number of clients and return completion times.
    def run_single_experiment(self, num_clients):
        print(f"Running experiment with {num_clients} clients...")
        
        # Create network
        net = create_network(num_clients=num_clients)
        
        try:
            # Get hosts
            server = net.get('server')
            clients = [net.get(f'client{i+1}') for i in range(num_clients)]
            
            # Start server
            server_proc = server.popen("python3 server.py")
            time.sleep(2)  # Give server time to start
            
            # Record start time
            start_time = time.time()
            
            # Start all clients simultaneously
            client_procs = []
            client_start_times = []
            
            for i in range(num_clients):
                client_start_time = time.time()
                proc = clients[i].popen(f"python3 client.py client{i+1} --quiet")
                client_procs.append(proc)
                client_start_times.append(client_start_time)
                time.sleep(0.05)  # Small delay to avoid exact simultaneity
            
            # Wait for all clients and record completion times
            completion_times = []
            for i, proc in enumerate(client_procs):
                proc.wait()
                client_end_time = time.time()
                completion_time = client_end_time - client_start_times[i]
                completion_times.append(completion_time)
                print(f"  Client {i+1} completed in {completion_time:.3f} seconds")
            
            # Stop server
            server_proc.terminate()
            server_proc.wait()
            
            return completion_times
            
        except Exception as e:
            print(f"Error in experiment: {e}")
            return None
        finally:
            net.stop()
            time.sleep(1)  # Cleanup time

    # Run complete analysis for Part 2.
    def run_analysis(self):
        print("Starting Part 2 Analysis...")
        print(f"Testing client numbers: {self.client_numbers}")
        print(f"Iterations per client count: {self.num_iterations}")
        
        results = []
        
        for num_clients in self.client_numbers:
            print(f"\n--- Testing {num_clients} clients ---")
            
            all_times_for_this_client_count = []
            
            # Run multiple iterations
            for iteration in range(self.num_iterations):
                print(f"  Iteration {iteration + 1}/{self.num_iterations}")
                
                completion_times = self.run_single_experiment(num_clients)
                if completion_times:
                    # Calculate average completion time for this iteration
                    avg_time = sum(completion_times) / len(completion_times)
                    all_times_for_this_client_count.append(avg_time)
                    print(f"    Average completion time: {avg_time:.3f} seconds")
            
            # Calculate statistics for this client count
            if all_times_for_this_client_count:
                avg_completion_time = sum(all_times_for_this_client_count) / len(all_times_for_this_client_count)
                std_dev = (sum([(t - avg_completion_time)**2 for t in all_times_for_this_client_count]) / len(all_times_for_this_client_count))**0.5
                
                # 95% confidence interval (approximation)
                confidence_interval = 1.96 * std_dev / (len(all_times_for_this_client_count)**0.5)
                
                result = {
                    'num_clients': num_clients,
                    'avg_completion_time': avg_completion_time,
                    'std_dev': std_dev,
                    'confidence_interval': confidence_interval,
                    'raw_times': all_times_for_this_client_count
                }
                
                results.append(result)
                
                print(f"  Summary for {num_clients} clients:")
                print(f"    Average: {avg_completion_time:.3f} ± {confidence_interval:.3f} seconds")
        
        return results

    # Save results to CSV file
    def save_results_to_csv(self, results):
        filename = 'part2_results.csv'
        
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = ['num_clients', 'avg_completion_time', 'std_dev', 'confidence_interval']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for result in results:
                writer.writerow({
                    'num_clients': result['num_clients'],
                    'avg_completion_time': result['avg_completion_time'],
                    'std_dev': result['std_dev'],
                    'confidence_interval': result['confidence_interval']
                })
        
        print(f"\nResults saved to {filename}")
        return filename

if __name__ == "__main__":
    runner = Part2Runner()
    results = runner.run_analysis()
    
    if results:
        runner.save_results_to_csv(results)
        print("\nPart 2 Done")
        
        # Print summary
        print("\nSUMMARY")
        for result in results:
            print(f"{result['num_clients']:2d} clients: {result['avg_completion_time']:.3f} ± {result['confidence_interval']:.3f} seconds")
    else:
        print("Analysis failed")
