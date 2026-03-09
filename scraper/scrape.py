import pandas as pd
from datetime import datetime
from pathlib import Path



RAW_FILE = Path("docs/data/data_raw.parquet")

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re


def get_station_data(station_id):

    url = f"https://climatologia.meteochile.gob.cl/application/diariob/visorDeDatosEma/{station_id}"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    # -------- TIMESTAMP REAL --------
    # bloque tipo:
    # <h1 class="text-danger">23:00 <small>04 Mar 2026</small></h1>

    ts_block = soup.find("h1", class_="text-danger")

    hora = ts_block.contents[0].strip()
    fecha = ts_block.find("small").text.strip()

    timestamp = datetime.strptime(
        f"{fecha} {hora}",
        "%d %b %Y %H:%M"
    )

    # -------- TEMPERATURA --------
    temp = None
    m = re.search(r'(-?\d+\.?\d*)\s*°?C', r.text)
    if m:
        temp = float(m.group(1))

    # -------- VIENTO --------
    wind = None
    wind_match = re.search(r'Instantáneo.*?(\d+)', r.text)
    if wind_match:
        wind = int(wind_match.group(1))

    # -------- PRECIP --------
    precip = 0
    rain_match = re.search(r'Hoy.*?(\d+\.?\d*)', r.text)
    if rain_match:
        precip = float(rain_match.group(1))

    return {
        "timestamp": timestamp,
        "temp": temp,
        "wind": wind,
        "precip": precip
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
        "precip": d["precip"]
    })

new_df = pd.DataFrame(rows)

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





