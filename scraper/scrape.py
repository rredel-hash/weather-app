import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import requests
import re
from bs4 import BeautifulSoup

RAW_FILE = Path("docs/data/data_raw.parquet")

def get_station_data(station_id):

    url = f"https://climatologia.meteochile.gob.cl/application/diariob/visorDeDatosEma/{station_id}"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()

    html = r.text
    soup = BeautifulSoup(html, 'html.parser')

    today_date = datetime.now().date()
    yesterday_date = today_date - timedelta(days=1)

    # -------- TIMESTAMP (current reading) --------
    timestamp = None
    ts_match = re.search(
        r'<h1[^>]*>\s*(\d{1,2}:\d{2})\s*<small>\s*([0-9]{1,2}\s+\w+\s+\d{4})',
        html,
        re.S
    )
    if ts_match:
        hora = ts_match.group(1)
        fecha = ts_match.group(2)
        timestamp = datetime.strptime(
            f"{fecha} {hora}",
            "%d %b %Y %H:%M"
        )

    # -------- TEMPERATURA ACTUAL --------
    temp = None
    temp_match = re.search(
        r'display-1">\s*([\-0-9\.]+)',
        html
    )
    if temp_match:
        temp = float(temp_match.group(1))

    # -------- VIENTO INSTANTÁNEO --------
    wind = None
    angulo_viento = None
    wind_match = re.search(
        r'Instantáneo.*?(\d{1,3})\/(\d{1,3})',
        html,
        re.S
    )
    if wind_match:
        angulo_viento = int(wind_match.group(1))
        wind = int(wind_match.group(2))

    # -------- PRECIP (tabla 6h) --------
    precip = 0.0
    rain_rows = re.findall(
        r'(\d{2}-\d{2}-\d{4}).*?(\d{2}:\d{2}).*?>\s*([0-9\.]+|s/p)\s*<',
        html,
        re.S
    )
    rain_rows = rain_rows[:10]
    for _, _, mm in rain_rows:
        if mm != "s/p":
            precip += float(mm)

    # -------- TEMPERATURA MÁXIMA Y MÍNIMA --------
    temp_maxima = None
    temp_minima = None
    temp_maxima_full_datetime = None # New variable for full datetime object
    temp_minima_full_datetime = None # New variable for full datetime object

    # Find the specific table for min/max temperatures
    summary_table = None
    for table_candidate in soup.find_all('table', class_='table-condensed'):
        headers_row = table_candidate.find('tr', class_='bg-rotulo-tabla')
        if headers_row and 'Mínima' in headers_row.get_text() and 'Máxima' in headers_row.get_text():
            summary_table = table_candidate
            break

    if summary_table:
        rows = summary_table.find_all('tr')
        for row in rows:
            th_h4_label = row.find('th').find('h4') if row.find('th') else None
            if not th_h4_label:
                continue

            label_text = th_h4_label.get_text(strip=True)
            current_date_for_temps = None

            if "Hoy" in label_text:
                current_date_for_temps = today_date
            elif "Ayer" in label_text: # Fallback for 'Ayer'
                current_date_for_temps = yesterday_date

            if current_date_for_temps: # Only process if we have a date context
                # Prioritize "Hoy" data, then fallback to "Ayer" if "Hoy" was not found yet
                # The loop structure already handles prioritization by breaking on "Hoy"

                tds = row.find_all('td')
                if len(tds) >= 2:
                    # Extract Minima
                    min_h4 = tds[0].find('h4')
                    if min_h4:
                        min_str_parts = min_h4.get_text(separator=' ', strip=True).split(' ')
                        min_str = min_str_parts[0]
                        temp_minima = float(min_str.replace(',', '.')) if min_str not in ('s/p', '--', '&nbsp;') else None
                        min_small = min_h4.find('small')
                        temp_minima_time_str = min_small.get_text(strip=True).replace(' Local', '') if min_small else None

                        if temp_minima is not None and temp_minima_time_str:
                            try:
                                time_obj = datetime.strptime(temp_minima_time_str, "%H:%M").time()
                                temp_minima_full_datetime = datetime.combine(current_date_for_temps, time_obj)
                            except ValueError:
                                temp_minima_full_datetime = None

                    # Extract Maxima
                    max_h4 = tds[1].find('h4')
                    if max_h4:
                        max_str_parts = max_h4.get_text(separator=' ', strip=True).split(' ')
                        max_str = max_str_parts[0]
                        temp_maxima = float(max_str.replace(',', '.')) if max_str not in ('s/p', '--', '&nbsp;') else None
                        max_small = max_h4.find('small')
                        temp_maxima_time_str = max_small.get_text(strip=True).replace(' Local', '') if max_small else None

                        if temp_maxima is not None and temp_maxima_time_str:
                            try:
                                time_obj = datetime.strptime(temp_maxima_time_str, "%H:%M").time()
                                temp_maxima_full_datetime = datetime.combine(current_date_for_temps, time_obj)
                            except ValueError:
                                temp_maxima_full_datetime = None

                    if "Hoy" in label_text: # If "Hoy" data was found, we are done
                        break

    return {
        "timestamp": timestamp,
        "temp": temp,
        "wind": wind,
        "precip": precip,
        "angulo_viento": angulo_viento,
        "temp_maxima": temp_maxima,
        "temp_minima": temp_minima,
        "temp_maxima_full_datetime": temp_maxima_full_datetime, # New full datetime object
        "temp_minima_full_datetime": temp_minima_full_datetime, # New full datetime object
    }


stations = []

with open("config/stations.txt") as f:
    for line in f:
        if "|" in line:
            sid, name = line.split("|")
            stations.append(sid.strip())

all_final_records = []

# Load existing data for comparison
df_existing = pd.DataFrame()
if RAW_FILE.exists():
    df_existing = pd.read_parquet(RAW_FILE)
    # Ensure datetime columns are actually datetime objects for proper comparison
    for col in ['timestamp', 'temp_maxima_full_datetime', 'temp_minima_full_datetime']:
        if col in df_existing.columns:
            df_existing[col] = pd.to_datetime(df_existing[col], errors='coerce')

for sid in stations:
    d = get_station_data(sid)

    # --- Generate main record (tipo_dato=0) --- General purpose observation
    main_record = {
        "station_id": sid,
        "timestamp": d["timestamp"],
        "temp": d["temp"],
        "wind": d["wind"],
        "precip": d["precip"],
        "angulo_viento": d["angulo_viento"],
        "temp_maxima": d["temp_maxima"],
        "temp_minima": d["temp_minima"],
        "tipo_dato": 0 # Main record is always tipo_dato=0 for current conditions
    }
    all_final_records.append(main_record)

    # --- Retrieve last record for comparison --- 
    last_record_for_sid = None
    if not df_existing.empty:
        # Filter for the current station_id and sort by timestamp to get the latest
        last_records_station = df_existing[df_existing['station_id'] == sid].sort_values('timestamp')
        if not last_records_station.empty:
            last_record_for_sid = last_records_station.iloc[-1]

    # --- Compare and add additional records for temp_maxima (tipo_dato=1) ---
    # Ensure we have scraped data and a valid full datetime for comparison
    if d["temp_maxima"] is not None and d["temp_maxima_full_datetime"] is not None:
        last_max_temp = None
        # Check if last_record_for_sid exists and has 'temp_maxima'
        if last_record_for_sid is not None and 'temp_maxima' in last_record_for_sid:
            last_max_temp = last_record_for_sid['temp_maxima']
            
        # Add event record if temp_maxima changed
        if last_max_temp != d["temp_maxima"]:
            additional_max_record = {
                "station_id": sid,
                "timestamp": d["temp_maxima_full_datetime"], # Use full datetime for timestamp of event
                "temp": d["temp_maxima"], # Store temp_maxima as 'temp' for this event record
                "wind": None,
                "precip": None,
                "angulo_viento": None,
                "temp_maxima": d["temp_maxima"], # Keep for context
                "temp_minima": None,
                "tipo_dato": 1 # Indicates a change event for temp_maxima
            }
            all_final_records.append(additional_max_record)

    # --- Compare and add additional records for temp_minima (tipo_dato=1) ---
    # Ensure we have scraped data and a valid full datetime for comparison
    if d["temp_minima"] is not None and d["temp_minima_full_datetime"] is not None:
        last_min_temp = None
        # Check if last_record_for_sid exists and has 'temp_minima'
        if last_record_for_sid is not None and 'temp_minima' in last_record_for_sid:
            last_min_temp = last_record_for_sid['temp_minima']

        # Add event record if temp_minima changed
        if last_min_temp != d["temp_minima"]:
            additional_min_record = {
                "station_id": sid,
                "timestamp": d["temp_minima_full_datetime"], # Use full datetime for timestamp of event
                "temp": d["temp_minima"], # Store temp_minima as 'temp' for this event record
                "wind": None,
                "precip": None,
                "angulo_viento": None,
                "temp_maxima": None,
                "temp_minima": d["temp_minima"], # Keep for context
                "tipo_dato": 1 # Indicates a change event for temp_minima
            }
            all_final_records.append(additional_min_record)

# Create DataFrame from all collected records
new_df = pd.DataFrame(all_final_records)

# Combine with existing data
if not df_existing.empty:
    df = pd.concat([df_existing, new_df])
else:
    df = new_df

# Handle duplicates based on station, timestamp, and tipo_dato
# This ensures that a main record and an event record at the same time are kept if distinct by tipo_dato
df.drop_duplicates(
    subset=["station_id","timestamp", "tipo_dato"],
    inplace=True, keep='last'
)

# Optional: Keep only most recent data (e.g., last 7 days)
# Make sure timestamp is datetime before comparison
df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
cutoff = pd.Timestamp.now() - pd.Timedelta(days=7)
df = df[df.timestamp >= cutoff]

import os
os.makedirs("docs/data", exist_ok=True)

df.to_parquet(RAW_FILE, index=False)