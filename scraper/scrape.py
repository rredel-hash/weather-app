import pandas as pd
from datetime import datetime
from pathlib import Path



RAW_FILE = Path("docs/data/data_raw.parquet")

def get_station_data(station_id):
    # <<< REEMPLAZAR CON TU SCRAPER REAL >>>
    return {
        "timestamp": datetime.now(),
        "temp": 18.3,
        "wind": 12,
        "precip": 0
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
os.makedirs("data", exist_ok=True)

df.to_parquet(RAW_FILE, index=False)


