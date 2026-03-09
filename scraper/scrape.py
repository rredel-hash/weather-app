import pandas as pd
from datetime import datetime
from pathlib import Path



RAW_FILE = Path("docs/data/data_raw.parquet")

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re

def get_station_data(station_id):

    url = f"https://tu_url_real_estacion/{station_id}"

    r = requests.get(url, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    # -------- TIMESTAMP REAL --------
    ts_block = soup.find("h1", class_="text-danger")

    hora = ts_block.contents[0].strip()
    fecha = ts_block.find("small").text.strip()

    timestamp = datetime.strptime(
        f"{fecha} {hora}",
        "%d %b %Y %H:%M"
    )

    # -------- TEMPERATURA --------
    temp = None
    temp_match = re.search(r'(-?\d+\.?\d*)\s*°?C', r.text)
    if temp_match:
        temp = float(temp_match.group(1))

    # -------- VIENTO --------
    wind = None
    wind_row = soup.find(string=re.compile("Instantáneo"))
    if wind_row:
        row = wind_row.find_parent("tr")
        wind_text = row.find_all("td")[1].text
        wind = int(wind_text.split("/")[1])

    # -------- PRECIP --------
    precip = 0
    rain_row = soup.find(string=re.compile("Hoy"))
    if rain_row:
        row = rain_row.find_parent("tr")
        val = row.find_all("td")[1].text.strip()
        if val not in ["s/p", "."]:
            precip = float(val)

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




