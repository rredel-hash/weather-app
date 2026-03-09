import pandas as pd
import json
from pathlib import Path


RAW = Path("docs/data/data_raw.parquet")
df = pd.read_parquet(RAW)

df["date"] = df.timestamp.dt.date
df["hour"] = df.timestamp.dt.strftime("%H:%M")

stations = df.station_id.unique()

for sid in stations:

    sdf = df[df.station_id == sid]

    latest = sdf.sort_values("timestamp").iloc[-1]

    outdir = Path(f"data/{sid}")
    outdir.mkdir(exist_ok=True)

    current = {
        "temp_now": float(latest.temp),
        "timestamp": str(latest.timestamp),
        "tmin_today": float(sdf.temp.min()),
        "tmax_today": float(sdf.temp.max()),
        "precip_today": float(sdf.precip.max())
    }

    with open(outdir/"current.json","w") as f:
        json.dump(current,f)

    daily = (
        sdf.groupby("date")
        .agg(
            tmin=("temp","min"),
            tmax=("temp","max"),
            precip=("precip","max"),
            wind_max=("wind","max")
        )
        .tail(5)
        .reset_index()
    )

    daily["complete"] = True
    daily.loc[daily.index[-1],"complete"]=False

    with open(outdir/"daily_5d.json","w") as f:
        json.dump({"days":daily.to_dict("records")},f)

