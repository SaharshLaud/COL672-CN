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
        
        self.num_clients = self.config.get('num_clients', 10)
        self.c_start = self.config.get('c', 1)
        print(f"Configuration: {self.num_clients} clients, starting c={self.c_start}")

    def cleanup_logs(self):
        if not os.path.exists('logs'): os.makedirs('logs')
        for f in glob.glob("logs/*"): os.remove(f)
        print("Cleaned old logs.")

    def parse_logs(self):
        print("Parsing completion logs...")
        times, rogue_times, normal_times = [], [], []
        for f in glob.glob("logs/*_completion.txt"):
            with open(f, 'r') as file:
                t = float(file.read().strip())
                times.append(t)
                if 'rogue' in f: rogue_times.append(t)
                else: normal_times.append(t)
        
        print(f"Parsed {len(times)} logs. Rogue: {np.mean(rogue_times):.2f}ms, Normal Avg: {np.mean(normal_times):.2f}ms")
        return times

    def calculate_jfi(self, times):
        if not times or len(times) < self.num_clients: return 0.0
        utilities = [1000.0 / t for t in times if t > 0]
        if not utilities or len(utilities) != self.num_clients: return 0.0
        n, sum_u, sum_u_sq = len(utilities), sum(utilities), sum(u**2 for u in utilities)
        return (sum_u**2) / (n * sum_u_sq) if (n * sum_u_sq) > 0 else 0.0

    def run_experiment(self, c_value):
        print(f"\n{'='*50}\n=== Running experiment with c = {c_value} (Round-Robin) ===\n{'='*50}")
        self.cleanup_logs()
        
        from topology import create_network
        net = create_network(num_clients=self.num_clients)
        
        try:
            server = net.get('server')
            clients = [net.get(f'client{i+1}') for i in range(self.num_clients)]
            
            print("Starting server...")
            server_proc = server.popen(["python3", "server.py"])
            time.sleep(2)

            print("Starting clients...")
            procs = [clients[0].popen(["python3", "client.py", "--batch-size", str(c_value), "--client-id", "rogue"])]
            procs.extend([c.popen(["python3", "client.py", "--batch-size", "1", "--client-id", f"normal_{i+1}"]) for i, c in enumerate(clients[1:])])
            print("All clients started, waiting for completion...")
            
            exit_codes = [p.wait() for p in procs]
            print(f"Client exit codes: {exit_codes}")
            
            print("Stopping server...")
            server_proc.terminate()
            server_proc.wait()
            time.sleep(1)

            all_times = self.parse_logs()
            if len(all_times) == self.num_clients:
                jfi = self.calculate_jfi(all_times)
                print(f"SUCCESS: c={c_value}, JFI={jfi:.4f}")
                return {'c': c_value, 'jfi': jfi}
            else:
                print(f"FAILED: Expected {self.num_clients} results, got {len(all_times)}.")
                return None
        finally:
            net.stop()

    def run_and_plot(self):
        c_values = list(range(1, 11))
        results = []
        for c in c_values:
            result = self.run_experiment(c)
            if result: results.append(result)
            else: print(f"Skipping failed experiment for c={c}"); continue
            time.sleep(1)
        
        if results: self.plot_jfi_vs_c(results)
        else: print("\nNo experiments completed successfully.")

    def plot_jfi_vs_c(self, results):
        c_values = [r['c'] for r in results]
        jfi_values = [r['jfi'] for r in results]
        
        plt.figure(figsize=(12, 7))
        plt.plot(c_values, jfi_values, 'o-', color='#2ca02c', linewidth=2, markersize=8, label='Round-Robin Fairness')
        plt.xlabel('Greediness Factor (c)', fontsize=14)
        plt.ylabel("Jain's Fairness Index (JFI)", fontsize=14)
        plt.title("Fairness with Round-Robin Scheduling", fontsize=16)
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.xticks(np.arange(min(c_values), max(c_values) + 1, 1))
        plt.ylim(0.95, 1.005) # Zoom in on the high fairness values
        
        for c, jfi in zip(c_values, jfi_values):
            plt.text(c, jfi + 0.001, f'{jfi:.4f}', ha='center', fontsize=9)
            
        plt.legend()
        plt.tight_layout()
        plt.savefig('p4_plot.png')
        print("\nPlot successfully saved as p4_plot.png")

if __name__ == '__main__':
    Runner().run_and_plot()
