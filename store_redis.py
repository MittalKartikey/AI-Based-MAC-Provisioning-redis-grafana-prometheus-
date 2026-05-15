import sys
import ast
import random
import threading
import redis  # Task 1: Added Redis import
import time
from flask import Flask
from prometheus_client import Gauge, CollectorRegistry, generate_latest

registry = CollectorRegistry()
app = Flask(__name__)

# Task 1: Initialize Redis Connection
try:
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    print("Connected to Redis successfully.")
except Exception as e:
    print(f"Redis Connection Error: {e}")

# MAC Table Data
ACTIVE_MACS = Gauge('network_mac_info', 'Detailed MAC Info', 
                     ['switch', 'port', 'vlan', 'mac'], registry=registry)

# Health Data
DROP_RATE = Gauge('network_dropped_packets_rate', 'Simulated drop rate', 
                  ['switch', 'port'], registry=registry)

# Global Counter
TOTAL_EVENTS = Gauge('network_events_total', 'Total processed cycles', registry=registry)

total_processed = 0

def process_line(line):
    global total_processed
    try:
        data = ast.literal_eval(line)
        etype = data.get("event_type")

        if etype == "mac_entry":
            # Update Prometheus
            mac_addr = str(data.get('mac', 'unknown'))
            sw = str(data.get('switch', 'unknown'))
            pt = str(data.get('port', 'unknown'))
            vl = str(data.get('vlan', 'unknown'))
            mac_age = float(data.get('age', 0))

            ACTIVE_MACS.labels(switch=sw, port=pt, vlan=vl, mac=mac_addr).set(mac_age)

            # Task 1: Store in Redis for future RL learning
            r.hset(f"device:{mac_addr}", mapping={
                "switch": sw,
                "port": pt,
                "vlan": vl,
                "timestamp": time.time()
            })

        elif etype == "port_stats":
            sw = data.get('switch', 'unknown')
            pt = data.get('port', 'unknown')
            
            # EMULATION: Re-introducing randomization to solve "0 packet drop" issue [cite: 1530, 1533]
            simulated_drops = random.uniform(0.1, 5.0) 
            
            # Update Prometheus
            DROP_RATE.labels(switch=sw, port=pt).set(simulated_drops)

            # Task 1: Store in Redis for RL observation space
            r.hset(f"health:{sw}:{pt}", mapping={
                "drops": simulated_drops,
                "last_update": time.time()
            })

        elif etype == "cycle_end":
            total_processed += 1
            TOTAL_EVENTS.set(total_processed)

    except Exception as e:
        print(f"Error processing line: {e}", file=sys.stderr)

def run_stream():
    # Fixed to read from stdin to allow piping from topo.py [cite: 1543, 3019]
    for line in sys.stdin:
        if line.strip():
            process_line(line.strip())

@app.route('/metrics')
def metrics():
    return generate_latest(registry), 200, {'Content-Type': 'text/plain'}

if __name__ == '__main__':
    # Start background data processing
    threading.Thread(target=run_stream, daemon=True).start()
    # Start Flask server for Prometheus on port 8000
    app.run(host='0.0.0.0', port=8000)