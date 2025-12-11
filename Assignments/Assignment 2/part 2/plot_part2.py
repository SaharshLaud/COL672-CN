#!/usr/bin/env python3

import csv
import matplotlib.pyplot as plt
import numpy as np

def plot_results():
    """Read CSV and create plot"""
    try:
        # Read data
        num_clients = []
        avg_times = []
        confidence_intervals = []
        
        with open('part2_results.csv', 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                num_clients.append(int(row['num_clients']))
                avg_times.append(float(row['avg_completion_time']))
                confidence_intervals.append(float(row['confidence_interval']))
        
        # Create plot
        plt.figure(figsize=(10, 6))
        plt.errorbar(num_clients, avg_times, yerr=confidence_intervals, 
                    marker='o', capsize=5, capthick=2, linewidth=2, markersize=8)
        
        plt.xlabel('Number of Clients')
        plt.ylabel('Average Completion Time per Client (seconds)')
        plt.title('Part 2: Average Completion Time vs Number of Clients')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        # Save plot
        plt.savefig('p2_plot.png', dpi=300, bbox_inches='tight')
        print("Plot saved as p2_plot.png")
        plt.show()
        
    except FileNotFoundError:
        print("Error: part2_results.csv not found. Run analysis first.")
    except Exception as e:
        print(f"Error creating plot: {e}")

if __name__ == "__main__":
    plot_results()
