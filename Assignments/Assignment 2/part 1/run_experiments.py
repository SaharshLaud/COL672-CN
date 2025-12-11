# STARTER CODE ONLY. EDIT AS DESIRED
#!/usr/bin/env python3
import re
import time
import csv
import json
from pathlib import Path
from topo_wordcount import make_net

# Config
K_VALUES = [1, 2, 5, 10, 20, 50, 100, 200]
RUNS_PER_K = 5
SERVER_CMD = "./server --config config.json"
# We will pass k as an argument directly
CLIENT_CMD_TMPL = "./client --config config.json --quiet --k {k}" 
RESULTS_CSV = Path("results.csv")

def main():
    # Prepare CSV
    if RESULTS_CSV.exists():
        RESULTS_CSV.unlink() # Delete old results
    with RESULTS_CSV.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["k", "run", "elapsed_ms"])

    net = make_net()
    net.start()
    h1, h2 = net.get('h1'), net.get('h2') # client, server

    print("Starting server...")
    srv = h2.popen(SERVER_CMD, shell=True)
    time.sleep(1) # Give server time to bind

    try:
        for k in K_VALUES:
            print(f"Testing k={k}...")
            for r in range(1, RUNS_PER_K + 1):
                # Format the client command with the current value of k
                cmd = CLIENT_CMD_TMPL.format(k=k)
                
                out = h1.cmd(cmd)
                
                m = re.search(r"ELAPSED_MS:(\d+)", out)
                if not m:
                    print(f"[warn] No ELAPSED_MS found for k={k} run={r}. Raw:\n{out}")
                    continue
                
                ms = int(m.group(1))
                with RESULTS_CSV.open("a", newline="") as f:
                    csv.writer(f).writerow([k, r, ms])
                print(f"  k={k} run={r} elapsed_ms={ms}")
    finally:
        print("Stopping server and network...")
        srv.terminate()
        time.sleep(0.2)
        net.stop()

if __name__ == "__main__":
    main()
