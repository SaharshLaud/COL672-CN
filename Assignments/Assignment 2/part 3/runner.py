#!/usr/bin/env python3

import json
import os
import time
import glob
import numpy as np
import matplotlib.pyplot as plt
import sys

class Runner:
    def __init__(self, config_file='config.json'):
        with open(config_file, 'r') as f:
            self.config = json.load(f)
        
        self.server_ip = self.config['server_ip']
        self.port = self.config['port']
        self.num_clients = self.config['num_clients']
        self.c = self.config['c']  # Batch size for rogue client
        self.p = self.config['p']  # Offset (always 0) since we want to download the full file
        self.k = self.config['k']  # Words per request (always 5)
        
        print(f"Config: {self.num_clients} clients, c={self.c}, p={self.p}, k={self.k}")

    def cleanup_logs(self):
        """Clean old log files"""
        logs = glob.glob("logs/*.log")
        completion_files = glob.glob("logs/*_completion.txt")
        all_files = logs + completion_files
        
        for log_file in all_files:
            try:
                os.remove(log_file)
            except OSError:
                pass
        print("Cleaned old logs")

    def parse_logs(self):
        """
        TODO: IMPLEMENTED
        Parse log files and return completion times
        Return: {'rogue': [times], 'normal': [times]}
        """
        print("Parsing completion logs...")
        
        rogue_times = []
        normal_times = []
        
        # Look for completion files
        completion_files = glob.glob("logs/*_completion.txt")
        print(f"Found completion files: {completion_files}")
        
        for file_path in completion_files:
            filename = os.path.basename(file_path)
            
            try:
                with open(file_path, 'r') as f:
                    time_str = f.read().strip()
                    completion_time = float(time_str)
                    
                    if 'rogue' in filename:
                        rogue_times.append(completion_time)
                        print(f"Rogue client completion time: {completion_time:.2f}ms")
                    else:
                        normal_times.append(completion_time)
                        print(f"Normal client completion time: {completion_time:.2f}ms")
                        
            except (ValueError, FileNotFoundError) as e:
                print(f"Error reading {file_path}: {e}")
        
        result = {'rogue': rogue_times, 'normal': normal_times}
        print(f"Parsed results: {len(rogue_times)} rogue, {len(normal_times)} normal clients")
        
        return result

    def calculate_jfi(self, completion_times):
        """
        TODO: IMPLEMENTED
        Calculate Jain's Fairness Index
        Note: JFI runs under the - more is better policy;
        i.e., JFI's variable must represent a positive benefit measure (e.g., throughput, share of CPU, utility).
        Formula: JFI = (sum of utilities)^2 / (n * sum of utilities^2)
        """
        if not completion_times:
            return 0.0
        
        # Convert completion times to utilities (inverse of completion time)
        # Higher utility means better performance (shorter completion time)
        utilities = [1000.0 / time for time in completion_times]  # Scale by 1000 for better numerics
        
        n = len(utilities)
        sum_utilities = sum(utilities)
        sum_utilities_squared = sum(u**2 for u in utilities)
        
        if sum_utilities_squared == 0:
            return 0.0
        
        jfi = (sum_utilities**2) / (n * sum_utilities_squared)
        
        print(f"JFI calculation: n={n}, times={[f'{t:.2f}' for t in completion_times]}")
        print(f"  utilities={[f'{u:.4f}' for u in utilities]}")
        print(f"  sum_utilities={sum_utilities:.4f}, sum_utilities_squared={sum_utilities_squared:.4f}")
        print(f"  JFI={jfi:.4f}")
        
        return jfi

    def run_experiment(self, c_value):
        """Run single experiment with given c value"""
        print(f"\n=== Running experiment with c={c_value} ===")
        
        # Clean logs
        self.cleanup_logs()
        
        # Create network
        from topology import create_network
        net = create_network(num_clients=self.num_clients)
        
        try:
            # Get hosts
            server = net.get('server')
            clients = [net.get(f'client{i+1}') for i in range(self.num_clients)]
            
            print(f"Network created with {len(clients)} clients")
            
            # Start server (students create server.py)
            print("Starting server...")
            server_proc = server.popen("python3 server.py", shell=False)
            time.sleep(5)  # Give server more time to start
            
            # Test server connectivity
            print("Testing server connectivity...")
            test_result = server.cmd("netstat -ln | grep 8887")
            print(f"Server listening check: {test_result}")
            
            # Start clients
            print("Starting clients...")
            
            # Client 1 is rogue (batch size c)
            print(f"Starting rogue client with batch size {c_value}")
            rogue_proc = clients[0].popen(f"python3 client.py --batch-size {c_value} --client-id rogue", shell=False)
            
            # Clients 2-N are normal (batch size 1)
            normal_procs = []
            for i in range(1, self.num_clients):
                client_id = f"normal_{i+1}"
                print(f"Starting normal client {client_id}")
                proc = clients[i].popen(f"python3 client.py --batch-size 1 --client-id {client_id}", shell=False)
                normal_procs.append(proc)
            
            print("All clients started, waiting for completion...")
            
            # Wait for all clients with timeout
            print("Waiting for rogue client...")
            rogue_result = rogue_proc.wait()
            print(f"Rogue client completed with code: {rogue_result}")
            
            print("Waiting for normal clients...")
            for i, proc in enumerate(normal_procs):
                result = proc.wait()
                print(f"Normal client {i+2} completed with code: {result}")
            
            # Give some time for file writes
            time.sleep(2)
            
            # Stop server
            print("Stopping server...")
            server_proc.terminate()
            server_proc.wait()
            time.sleep(1)
            
            # Parse results
            print("Parsing results...")
            results = self.parse_logs()
            
            # Calculate JFI
            all_times = results['rogue'] + results['normal']
            if all_times and len(all_times) == self.num_clients:
                jfi = self.calculate_jfi(all_times)
                print(f"Experiment c={c_value}: JFI = {jfi:.4f}")
                
                # Save results for plotting
                result_data = {
                    'c': c_value,
                    'rogue_times': results['rogue'],
                    'normal_times': results['normal'],
                    'all_times': all_times,
                    'jfi': jfi
                }
                
                return result_data
            else:
                print(f"Incomplete results: expected {self.num_clients} completion times, got {len(all_times)}")
                print(f"Rogue times: {results['rogue']}")
                print(f"Normal times: {results['normal']}")
                return None
            
        except Exception as e:
            print(f"Error in experiment: {e}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            try:
                net.stop()
                time.sleep(1)
            except:
                pass

    def run_varying_c(self):
        """Run experiments with c starting from config value, incrementing by 2 until <= 20"""
        # Modified to run from 1 to 10 as per assignment requirements
        c_values = list(range(1, 11))  # 1 to 10 inclusive
        print(f"Running experiments with c values: {c_values}")
        
        results = []
        
        for c in c_values:
            print(f"\n{'='*50}")
            print(f"Testing c = {c}")
            print(f"{'='*50}")
            
            result = self.run_experiment(c)
            if result:
                results.append(result)
                print(f"Experiment with c={c} completed successfully")
            else:
                print(f"Experiment with c={c} failed")
            
            # Delay between experiments
            time.sleep(3)
        
        print("\nAll experiments completed")
        
        # Save results and plot
        if results:
            self.save_results(results)
            self.plot_jfi_vs_c(results)
        else:
            print("No successful experiments to plot")
        
        return results

    def save_results(self, results):
        """Save experimental results to files"""
        
        # Save CSV for plotting
        csv_data = []
        csv_data.append("c,jfi,avg_rogue_time,avg_normal_time,num_clients")
        
        for result in results:
            c = result['c']
            jfi = result['jfi']
            avg_rogue = np.mean(result['rogue_times']) if result['rogue_times'] else 0
            avg_normal = np.mean(result['normal_times']) if result['normal_times'] else 0
            num_clients = len(result['all_times'])
            
            csv_data.append(f"{c},{jfi:.4f},{avg_rogue:.2f},{avg_normal:.2f},{num_clients}")
        
        with open('results.csv', 'w') as f:
            f.write('\n'.join(csv_data))
        
        print("Results saved to results.csv")

    def plot_jfi_vs_c(self, results):
        """
        TODO: IMPLEMENTED
        Plot JFI values vs c values
        """
        print("Creating JFI vs c plot...")
        
        c_values = [r['c'] for r in results]
        jfi_values = [r['jfi'] for r in results]
        
        # Create the plot
        plt.figure(figsize=(10, 6))
        plt.plot(c_values, jfi_values, 'bo-', linewidth=2, markersize=8)
        plt.xlabel('c (Batch Size for Greedy Client)', fontsize=12)
        plt.ylabel('Jain Fairness Index (JFI)', fontsize=12)
        plt.title('Fairness vs Greedy Client Batch Size\n(FCFS Scheduling)', fontsize=14)
        plt.grid(True, alpha=0.3)
        plt.ylim(0, 1.1)
        plt.xlim(0.5, max(c_values) + 0.5)
        
        # Add value labels on points
        for i, (c, jfi) in enumerate(zip(c_values, jfi_values)):
            plt.annotate(f'{jfi:.3f}', (c, jfi), textcoords="offset points", 
                        xytext=(0,10), ha='center', fontsize=9)
        
        plt.tight_layout()
        plt.savefig('p3_plot.png', dpi=300, bbox_inches='tight')
        
        print("Plot saved as p3_plot.png")
        
        # Print summary
        print(f"\nExperiment Summary:")
        print(f"C values tested: {c_values}")
        print(f"JFI values: {[f'{jfi:.4f}' for jfi in jfi_values]}")
        if jfi_values:
            print(f"JFI range: {min(jfi_values):.4f} to {max(jfi_values):.4f}")

def main():
    runner = Runner()
    # Run experiments with varying c values
    results = runner.run_varying_c()
    
    if results:
        print(f"\nFinal Summary: Completed {len(results)} experiments")
        for result in results:
            print(f"c={result['c']}: JFI={result['jfi']:.4f}")
    else:
        print("No experiments completed successfully")

if __name__ == '__main__':
    main()
