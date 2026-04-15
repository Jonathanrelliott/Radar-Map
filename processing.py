import os
import time
import csv
import pyart
import numpy as np
from datetime import datetime

WATCH_DIR = 'radar_data'
CSV_FILE = 'hazards.csv'
THRESHOLD = 45.0

def process_latest():
    files = [os.path.join(WATCH_DIR, f) for f in os.listdir(WATCH_DIR) if not f.endswith('.csv')]
    if not files: return
    
    latest_file = max(files, key=os.path.getctime)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Processing: {os.path.basename(latest_file)}")

    try:
        radar = pyart.io.read_nexrad_archive(latest_file)
        data = radar.fields['reflectivity']['data']
        lats = radar.gate_latitude['data']
        lons = radar.gate_longitude['data']

        dist_per_gate = radar.range['meters_between_gates'] if 'meters_between_gates' in radar.range else 250
        inner_gates = int(5000 / dist_per_gate)
        
        storm_mask = (data > THRESHOLD)
        storm_mask[:, :inner_gates] = False 

        if not np.any(storm_mask):
            with open(CSV_FILE, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['severity_type', 'min_lat', 'max_lat', 'min_lon', 'max_lon', 'max_dbz'])
                writer.writeheader()
            print("Status: Clear")
            return

        s_lats, s_lons = lats[storm_mask], lons[storm_mask]
        max_v = np.max(data[storm_mask])
        severity = "SEVERE" if max_v > 60 else "MODERATE"

        hazard = {
            'severity_type': severity,
            'min_lat': np.min(s_lats), 'max_lat': np.max(s_lats),
            'min_lon': np.min(s_lons), 'max_lon': np.max(s_lons),
            'max_dbz': round(float(max_v), 1)
        }

        with open(CSV_FILE, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=hazard.keys())
            writer.writeheader()
            writer.writerow(hazard)
        print(f"Success: {severity} hazard identified.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if not os.path.exists(WATCH_DIR): os.makedirs(WATCH_DIR)
    print("Background Processor monitoring 'radar_data'...")
    last_mtime = 0
    while True:
        files = [os.path.join(WATCH_DIR, f) for f in os.listdir(WATCH_DIR) if not f.endswith('.csv')]
        if files:
            cur = max(files, key=os.path.getctime)
            mtime = os.path.getmtime(cur)
            if mtime > last_mtime:
                process_latest()
                last_mtime = mtime
        time.sleep(5)