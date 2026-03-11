import pandas as pd
from datetime import datetime
from pathlib import Path
# import numpy as np # Removed as angle is directly scraped

RAW_FILE = Path("docs/data/data_raw.parquet")

import requests
import re
# from datetime import datetime # Already imported above


def get_station_data(station_id):

    url = f"https://climatologia.meteochile.gob.cl/application/diariob/visorDeDatosEma/{station_id}"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()

    html = r.text

    # -------- TIMESTAMP --------

    timestamp = None

    ts_match = re.search(
        r'<h1[^>]*>\s*(\\d{1,2}:\\d{2})\s*<small>\s*([0-9]{1,2}\s+\w+\s+\\d{4})',
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

    # -------- TEMPERATURA --------

    temp = None

    temp_match = re.search(
        r'display-1">\s*([\\-0-9\\.]+)',
        html
    )

    if temp_match:
        temp = float(temp_match.group(1))

    # -------- VIENTO INSTANTÁNEO --------

    wind = None
    angulo_viento = None # Initialize new variable for wind angle

    wind_match = re.search(
        r'Instantáneo.*?(\\d{1,3})\\/(\\d{1,3})',
        html,
        re.S
    )

    if wind_match:
        angulo_viento = int(wind_match.group(1)) # Extract angle
        wind = int(wind_match.group(2)) # Extract wind speed

    # -------- PRECIP (tabla 6h) --------

    precip = 0.0

    rain_rows = re.findall(
        r'(\\d{2}-\\d{2}-\\d{4}).*?(\\d{2}:\\d{2}).*?>\\s*([0-9\\.]+|s/p)\\s*<',
        html,
        re.S
    )

    rain_rows = rain_rows[:10]

    for _, _, mm in rain_rows:

        if mm != "s/p":
            precip += float(mm)

    return {
        "timestamp": timestamp,
        "temp": temp,
        "wind": wind,
        "precip": precip,
        "angulo_viento": angulo_viento # Add angle to the return dict
    }


stations = []

with open("config/stations.txt") as f:
    for line in f:
        if "|" in line:
            sid, name = line.split("|")
            stations.append(sid.strip())

rows = []

for sid in stations:
    d = get_station_data(sid)

    rows.append({
        "station_id": sid,
        "timestamp": d["timestamp"],
        "temp": d["temp"],
        "wind": d["wind"],
        "precip": d["precip"],
        "angulo_viento": d["angulo_viento"] # Populate from the returned dict
    })

new_df = pd.DataFrame(rows)

# --- Lógica para agregar 'tipo_dato' --- 
# Se añade la columna 'tipo_dato' basada en si la precipitación es mayor que 0.
new_df['tipo_dato'] = (new_df['precip'] > 0).astype(int)
# 'angulo_viento' is now directly scraped, so no need for random generation here.
# new_df['angulo_viento'] = np.random.randint(0, 361, size=len(new_df)) # Removed

if RAW_FILE.exists():
    df = pd.read_parquet(RAW_FILE)
    df = pd.concat([df, new_df])
else:
    df = new_df

df.drop_duplicates(
    ["station_id","timestamp"],
    inplace=True
)

cutoff = pd.Timestamp.now() - pd.Timedelta(days=7)
df = df[df.timestamp >= cutoff]

import os
os.makedirs("docs/data", exist_ok=True)

df.to_parquet(RAW_FILE, index=False)