from flask import Flask, render_template, jsonify, send_file, request
import numpy as np
import pyart
import os
import io
import boto3
import botocore
from datetime import datetime, timezone, timedelta
import math
import matplotlib
import cartopy.crs as ccrs

matplotlib.use('Agg')
import matplotlib.pyplot as plt

app = Flask(__name__)

RADAR_DIR = 'radar_data'
BUCKET = 'unidata-nexrad-level2'
s3 = boto3.client('s3', config=botocore.config.Config(signature_version=botocore.UNSIGNED))

PRODUCT_MAP = {
    'N0Q': 'reflectivity',
    'N0U': 'velocity',
    'N0C': 'cross_correlation_ratio'
}

VALID_PRODUCTS = set(PRODUCT_MAP.keys())

RENDER_CONFIG = {
    'N0Q': {'cmap': 'NWSRef', 'vmin': -10, 'vmax': 75},
    'N0U': {'cmap': 'NWSVel', 'vmin': -45, 'vmax': 45},
    'N0C': {'cmap': 'NWS_CC', 'vmin': 0.7, 'vmax': 1.0}
}

def normalize_station(station):
    station = station.upper().strip()
    if len(station) == 4 and station.startswith('K'):
        return station
    if len(station) == 3 and station.isalpha():
        return f"K{station}"
    raise ValueError("Station must be 3-letter (e.g. MKX) or K-prefixed 4-letter (e.g. KMKX)")

def sweep_for_product(radar, product):
    if product == 'N0U':
        return min(1, radar.nsweeps - 1)
    return 0

def get_latest_s3_key(station):
    station = normalize_station(station)
    # Look back across a few UTC days in case station data is delayed.
    for day_offset in range(0, 3):
        day = datetime.now(timezone.utc) - timedelta(days=day_offset)
        prefix = f"{day:%Y/%m/%d}/{station}/"
        try:
            objs = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=500)
        except Exception:
            continue

        contents = objs.get('Contents', [])
        keys = sorted(obj['Key'] for obj in contents if not obj['Key'].endswith('_MDM'))
        if keys:
            return keys[-1]
    return None

def local_name_from_key(key):
    base = os.path.basename(key)
    if '.' in base:
        return base
    return f"{base}.ar2v"

def get_latest_local_file(station):
    if not os.path.isdir(RADAR_DIR):
        return None

    station = normalize_station(station)
    files = []
    for name in os.listdir(RADAR_DIR):
        full = os.path.join(RADAR_DIR, name)
        if not os.path.isfile(full):
            continue
        if name.endswith('.csv'):
            continue
        if station and not name.upper().startswith(station):
            continue
        files.append(full)

    if not files:
        return None
    return max(files, key=os.path.getmtime)

def get_local_file_by_name(station, file_name):
    if not file_name:
        return None

    station = normalize_station(station)
    safe_name = os.path.basename(file_name.strip())
    if not safe_name:
        return None
    if safe_name.endswith('.csv'):
        raise ValueError('CSV files are not valid radar volumes')
    if not safe_name.upper().startswith(station):
        raise ValueError(f"File '{safe_name}' does not belong to station {station}")

    full_path = os.path.join(RADAR_DIR, safe_name)
    if not os.path.isfile(full_path):
        return None
    return full_path

def list_local_files(station):
    if not os.path.isdir(RADAR_DIR):
        return []

    station = normalize_station(station)
    rows = []
    for name in os.listdir(RADAR_DIR):
        full = os.path.join(RADAR_DIR, name)
        if not os.path.isfile(full):
            continue
        if name.endswith('.csv'):
            continue
        if not name.upper().startswith(station):
            continue
        rows.append({
            'file': name,
            'mtime': os.path.getmtime(full),
        })

    rows.sort(key=lambda r: r['mtime'], reverse=True)
    return rows

def read_latest_radar(station, file_name=None):
    selected_file = get_local_file_by_name(station, file_name) if file_name else None
    latest_file = selected_file or get_latest_local_file(station)
    if not latest_file:
        return None, None
    radar = pyart.io.read_nexrad_archive(latest_file)
    return radar, latest_file

def km_width_height(bounds):
    min_lat, min_lon = bounds[0]
    max_lat, max_lon = bounds[1]
    mid_lat = (min_lat + max_lat) / 2.0
    height_km = max(0.0, (max_lat - min_lat) * 111.32)
    width_km = max(0.0, (max_lon - min_lon) * 111.32 * math.cos(math.radians(mid_lat)))
    return width_km, height_km

def extract_hazard_boxes(radar, max_boxes=80, lat_bins=20, lon_bins=20):
    refl_field = radar.fields.get('reflectivity')
    vel_field = radar.fields.get('velocity')
    cc_field = radar.fields.get('cross_correlation_ratio')
    if not refl_field or not vel_field or not cc_field:
        return []

    gate_lats = np.asarray(radar.gate_latitude['data'], dtype=float)
    gate_lons = np.asarray(radar.gate_longitude['data'], dtype=float)
    gate_lons = ((gate_lons + 180.0) % 360.0) - 180.0

    refl = np.ma.masked_invalid(np.ma.array(refl_field['data'], dtype=float))
    vel = np.ma.masked_invalid(np.ma.array(vel_field['data'], dtype=float))
    cc = np.ma.masked_invalid(np.ma.array(cc_field['data'], dtype=float))

    rows = min(gate_lats.shape[0], gate_lons.shape[0], refl.shape[0], vel.shape[0], cc.shape[0])
    cols = min(gate_lats.shape[1], gate_lons.shape[1], refl.shape[1], vel.shape[1], cc.shape[1])

    gate_lats = gate_lats[:rows, :cols]
    gate_lons = gate_lons[:rows, :cols]
    refl = refl[:rows, :cols]
    vel = vel[:rows, :cols]
    cc = cc[:rows, :cols]

    valid_geo = (
        np.isfinite(gate_lats)
        & np.isfinite(gate_lons)
        & (gate_lats >= -90.0)
        & (gate_lats <= 90.0)
        & (gate_lons >= -180.0)
        & (gate_lons <= 180.0)
    )
    valid = valid_geo & ~np.ma.getmaskarray(refl) & ~np.ma.getmaskarray(vel) & ~np.ma.getmaskarray(cc)
    if not np.any(valid):
        return []

    lat_values = gate_lats[valid]
    lon_values = gate_lons[valid]
    lat_edges = np.linspace(float(np.min(lat_values)), float(np.max(lat_values)), lat_bins + 1)
    lon_edges = np.linspace(float(np.min(lon_values)), float(np.max(lon_values)), lon_bins + 1)

    boxes = []
    for i in range(lat_bins):
        lat_min = float(lat_edges[i])
        lat_max = float(lat_edges[i + 1])
        for j in range(lon_bins):
            lon_min = float(lon_edges[j])
            lon_max = float(lon_edges[j + 1])
            cell = (
                valid
                & (gate_lats >= lat_min)
                & (gate_lats < lat_max if i < lat_bins - 1 else gate_lats <= lat_max)
                & (gate_lons >= lon_min)
                & (gate_lons < lon_max if j < lon_bins - 1 else gate_lons <= lon_max)
            )
            if not np.any(cell):
                continue

            refl_vals = np.asarray(refl[cell], dtype=float)
            vel_vals = np.asarray(np.abs(vel[cell]), dtype=float)
            cc_vals = np.asarray(cc[cell], dtype=float)

            refl_vals = refl_vals[np.isfinite(refl_vals)]
            vel_vals = vel_vals[np.isfinite(vel_vals)]
            cc_vals = cc_vals[np.isfinite(cc_vals)]
            if refl_vals.size == 0 or vel_vals.size == 0 or cc_vals.size == 0:
                continue

            refl_p90 = float(np.percentile(refl_vals, 90))
            vel_p90 = float(np.percentile(vel_vals, 90))
            cc_p10 = float(np.percentile(cc_vals, 10))

            hazard_type = None
            color = None
            score = 0.0
            if vel_p90 >= 28.0 and refl_p90 >= 40.0 and cc_p10 <= 0.93:
                hazard_type = 'tornado_signature'
                color = '#dc2626'
                score = vel_p90 + (0.95 - cc_p10) * 120.0
            elif refl_p90 >= 52.0 and vel_p90 >= 25.0:
                hazard_type = 'severe_storm'
                color = '#f97316'
                score = refl_p90 + vel_p90
            elif refl_p90 >= 46.0:
                hazard_type = 'water_hazard'
                color = '#22c55e'
                score = refl_p90
            else:
                continue

            bounds = [[lat_min, lon_min], [lat_max, lon_max]]
            center_lat = (lat_min + lat_max) / 2.0
            center_lon = (lon_min + lon_max) / 2.0
            width_km, height_km = km_width_height(bounds)

            boxes.append({
                'hazard_type': hazard_type,
                'color': color,
                'score': round(score, 2),
                'bounds': bounds,
                'center': [round(center_lat, 4), round(center_lon, 4)],
                'dimensions_km': {'width': round(width_km, 2), 'height': round(height_km, 2)},
                'metrics': {
                    'reflectivity_p90': round(refl_p90, 2),
                    'velocity_abs_p90': round(vel_p90, 2),
                    'cc_p10': round(cc_p10, 4),
                },
            })

    boxes.sort(key=lambda b: (b['hazard_type'] == 'tornado_signature', b['score']), reverse=True)
    return boxes[:max_boxes]

@app.route('/api/download/<station>')
def download_latest(station):
    try:
        station = normalize_station(station)
        os.makedirs(RADAR_DIR, exist_ok=True)
        key = get_latest_s3_key(station)
        if not key:
            return jsonify({'error': f'No S3 data found for station {station}'}), 404

        local_name = local_name_from_key(key)
        local_path = os.path.join(RADAR_DIR, local_name)

        if not os.path.exists(local_path):
            s3.download_file(BUCKET, key, local_path)

        return jsonify({
            'station': station,
            'key': key,
            'file': local_name,
            'path': local_path,
            'status': 'ready'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/metadata/<station>')
def metadata(station):
    try:
        station = normalize_station(station)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    selected_file = (request.args.get('file') or '').strip()
    try:
        radar, latest_file = read_latest_radar(station, selected_file)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    if not latest_file:
        return jsonify({'error': 'No local radar file found in radar_data'}), 404
    try:
        lat, lon = radar.gate_latitude['data'], radar.gate_longitude['data']
        lon = ((lon + 180) % 360) - 180
        bounds = [[float(np.min(lat)), float(np.min(lon))], [float(np.max(lat)), float(np.max(lon))]]
        center = [float(radar.latitude['data'][0]), float(radar.longitude['data'][0])]
        return jsonify({
            'bounds': bounds, 
            'center': center,
            'time': os.path.basename(latest_file)
        })
    except Exception as e: return jsonify({'error': str(e)}), 500

@app.route('/api/hazards/<station>')
def hazards(station):
    try:
        station = normalize_station(station)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    selected_file = (request.args.get('file') or '').strip()
    try:
        radar, latest_file = read_latest_radar(station, selected_file)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    if not latest_file:
        return jsonify({'error': 'No local radar file found in radar_data'}), 404

    try:
        hazard_filter = (request.args.get('kind', 'all') or 'all').strip().lower()
    except Exception:
        hazard_filter = 'all'

    try:
        boxes = extract_hazard_boxes(radar)
        if hazard_filter != 'all':
            boxes = [b for b in boxes if b['hazard_type'] == hazard_filter]
        return jsonify({
            'station': station,
            'time': os.path.basename(latest_file),
            'count': len(boxes),
            'boxes': boxes,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/files/<station>')
def files(station):
    try:
        station = normalize_station(station)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    try:
        rows = list_local_files(station)
        return jsonify({
            'station': station,
            'count': len(rows),
            'files': rows,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/render/<station>/<product>')
def render(station, product):
    product = product.upper().strip()
    if product not in VALID_PRODUCTS:
        return jsonify({'error': f"Unsupported product '{product}'"}), 400

    try:
        station = normalize_station(station)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    selected_file = (request.args.get('file') or '').strip()
    try:
        radar, latest_file = read_latest_radar(station, selected_file)
    except ValueError as e:
        return str(e), 400
    if not latest_file:
        return "No local radar file found in radar_data", 404
    try:
        field = PRODUCT_MAP[product]
        if field not in radar.fields:
            return f"Field '{field}' unavailable for this volume", 404
        cfg = RENDER_CONFIG[product]
        sweep = sweep_for_product(radar, product)

        # Filter and Plot
        gatefilter = pyart.filters.GateFilter(radar)
        gatefilter.exclude_transition()
        gatefilter.exclude_masked(field)
        gatefilter.exclude_invalid(field)
        # Suppress known near-radar contamination ring with a very small inner mask.
        ranges = np.asarray(radar.range['data'], dtype=float)
        inner_mask = ranges < 2200.0
        if np.any(inner_mask):
            m = np.broadcast_to(inner_mask, radar.fields[field]['data'].shape)
            gatefilter.exclude_gates(m)

        fig = plt.figure(figsize=(12, 12), dpi=150)
        ax = fig.add_axes([0, 0, 1, 1], projection=ccrs.PlateCarree(), facecolor='none')
        ax.set_axis_off()

        display = pyart.graph.RadarMapDisplay(radar)
        display.plot_ppi_map(field, sweep, vmin=cfg['vmin'], vmax=cfg['vmax'], 
                             cmap=cfg['cmap'], ax=ax, colorbar_flag=False, 
                             title_flag=False, embellish=False, gatefilter=gatefilter,
                             add_grid_lines=False, edges=False, filter_transitions=False,
                             edgecolors='none', antialiased=False, linewidth=0, raster=True)

        img_io = io.BytesIO()
        fig.savefig(img_io, format='png', transparent=True, bbox_inches='tight', pad_inches=0)
        plt.close(fig)
        img_io.seek(0)
        return send_file(img_io, mimetype='image/png')
    except Exception as e: return str(e), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0",debug=True, port=5555)
