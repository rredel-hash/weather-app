import pandas as pd
from datetime import datetime
from pathlib import Path
import requests
import re
from bs4 import BeautifulSoup # Added BeautifulSoup import

RAW_FILE = Path("docs/data/data_raw.parquet")

def get_station_data(station_id):

    url = f"https://climatologia.meteochile.gob.cl/application/diariob/visorDeDatosEma/{station_id}"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()

    html = r.text
    soup = BeautifulSoup(html, 'html.parser') # Create BeautifulSoup object

    # -------- TIMESTAMP --------
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
    temp_maxima_timestamp = None
    temp_minima_timestamp = None

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

            if "Hoy" in label_text or ("Ayer" in label_text and temp_maxima is None):
                tds = row.find_all('td')
                if len(tds) >= 2:
                    min_h4 = tds[0].find('h4')
                    if min_h4:
                        min_str_parts = min_h4.get_text(separator=' ', strip=True).split(' ')
                        min_str = min_str_parts[0]
                        temp_minima = float(min_str.replace(',', '.')) if min_str not in ('s/p', '--', '&nbsp;') else None
                        min_small = min_h4.find('small')
                        temp_minima_timestamp = min_small.get_text(strip=True) if min_small else None

                    max_h4 = tds[1].find('h4')
                    if max_h4:
                        max_str_parts = max_h4.get_text(separator=' ', strip=True).split(' ')
                        max_str = max_str_parts[0]
                        temp_maxima = float(max_str.replace(',', '.')) if max_str not in ('s/p', '--', '&nbsp;') else None
                        max_small = max_h4.find('small')
                        temp_maxima_timestamp = max_small.get_text(strip=True) if max_small else None

                    if "Hoy" in label_text:
                        break

    return {
        "timestamp": timestamp,
        "temp": temp,
        "wind": wind,
        "precip": precip,
        "angulo_viento": angulo_viento,
        "temp_maxima": temp_maxima, # Added
        "temp_minima": temp_minima, # Added
        "temp_maxima_timestamp": temp_maxima_timestamp, # Added
        "temp_minima_timestamp": temp_minima_timestamp # Added
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
        "angulo_viento": d["angulo_viento"],
        "temp_maxima": d["temp_maxima"], # Added
        "temp_minima": d["temp_minima"], # Added
        "temp_maxima_timestamp": d["temp_maxima_timestamp"], # Added
        "temp_minima_timestamp": d["temp_minima_timestamp"] # Added
    })

new_df = pd.DataFrame(rows)

# --- Lógica para agregar 'tipo_dato' (actualizada) --- #
tipo_dato_calculated = []
for index, row in new_df.iterrows():
    if row['temp_maxima'] is not None and row['temp_minima'] is not None:
        if (row['temp_maxima'] - row['temp_minima']) >= 5.0:
            tipo_dato_calculated.append(1)
        else:
            tipo_dato_calculated.append(0)
    else:
        tipo_dato_calculated.append(0) # Default to 0 if temp_maxima or temp_minima are None
new_df['tipo_dato'] = tipo_dato_calculated


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