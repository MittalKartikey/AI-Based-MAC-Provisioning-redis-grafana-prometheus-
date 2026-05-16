import sys
import ast
import random
import threading
from flask import Flask
from prometheus_client import Gauge, CollectorRegistry, generate_latest

registry = CollectorRegistry()
app = Flask(__name__)

# MAC Tabel Data
ACTIVE_MACS = Gauge('network_mac_info', 'Detailed MAC Info', 
                     ['switch', 'port', 'vlan', 'mac'], registry=registry)

# Health Data
DROP_RATE = Gauge('network_dropped_packets_rate', 'Simulated drop rate', 
                  ['switch', 'port'], registry=registry)

#Global Counter
TOTAL_EVENTS = Gauge('network_events_total', 'Total processed cycles', registry=registry)

total_processed = 0

def process_line(line):
    global total_processed
    try:
        data = ast.literal_eval(line)
        etype = data.get("event_type")

        if etype == "mac_entry":
            # Update MAC Table info
            mac_age = float(data.get('age', 0))
            ACTIVE_MACS.labels(
                switch=str(data.get('switch', 'unknown')), 
                port=str(data.get('port', 'unknown')), 
                vlan=str(data.get('vlan', 'unknown')), 
                mac=str(data.get('mac', 'unknown'))
            ).set(mac_age)

        elif etype == "port_stats":
            DROP_RATE.labels(
                switch=data.get('switch', 'unknown'), 
                port=data.get('port', 'unknown')
            ).set(random.uniform(0, 5))

        elif etype == "cycle_end":
            total_processed += 1
            TOTAL_EVENTS.set(total_processed)

    except Exception as e:
        print(f"Error processing line: {e}", file=sys.stderr)

def run_stream():
    for line in sys.stdin:
        if line.strip():
            process_line(line.strip())

@app.route('/metrics')
def metrics():
    return generate_latest(registry), 200, {'Content-Type': 'text/plain'}

if __name__ == '__main__':
    threading.Thread(target=run_stream, daemon=True).start()
    app.run(host='0.0.0.0', port=8000)