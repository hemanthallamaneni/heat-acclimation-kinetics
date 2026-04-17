"""
Fetch hourly weather from Open-Meteo Historical for every run in
mart_run_samples. Caches to data/raw/weather.parquet.

One API call per (run start date, first GPS coordinate). Open-Meteo is
free for non-commercial use, no API key. Hourly resolution; we pull a
~6-hour window around each run for downstream interpolation.
"""

import os
import sys
import time
import pandas as pd
import requests
from dotenv import load_dotenv
import snowflake.connector

# Reuse the platform's snowflake auth helper
PLATFORM_PATH = os.path.expanduser("~/work/personal/health-analytics-platform")
sys.path.insert(0, PLATFORM_PATH)
from ingestion.common.snowflake_auth import load_private_key

load_dotenv(os.path.join(PLATFORM_PATH, ".env"))

OUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "weather.parquet")
HOURLY_VARS = "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,wind_speed_10m,precipitation"


def get_runs():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=load_private_key(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="MART_HEALTH",
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT
            activity_id,
            local_date,
            first_lat,
            first_lon,
            start_date
        FROM mart_run_samples
        QUALIFY ROW_NUMBER() OVER (PARTITION BY activity_id ORDER BY sample_index) = 1
        ORDER BY local_date
    """)
    cols = [c[0].lower() for c in cur.description]
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=cols)


def fetch_weather_for_run(run):
    """One API call per run. Pulls the full local day, hourly."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": float(run["first_lat"]),
        "longitude": float(run["first_lon"]),
        "start_date": run["local_date"].isoformat(),
        "end_date": run["local_date"].isoformat(),
        "hourly": HOURLY_VARS,
        "timezone": "America/Chicago",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    j = resp.json()
    h = j["hourly"]
    df = pd.DataFrame(h)
    df["activity_id"] = run["activity_id"]
    df["local_date"] = run["local_date"]
    return df


if __name__ == "__main__":
    runs = get_runs()
    print(f"Fetching weather for {len(runs)} runs...")

    frames = []
    for i, run in runs.iterrows():
        try:
            df = fetch_weather_for_run(run)
            frames.append(df)
            print(f"  [{i+1}/{len(runs)}] {run['local_date']} {run['activity_id']}: {len(df)} hourly rows")
        except Exception as e:
            print(f"  [{i+1}/{len(runs)}] ERROR: {e}")
        # Open-Meteo throttle: free tier ~10k calls/day, we're using <50 so be nice
        time.sleep(0.1)

    out = pd.concat(frames, ignore_index=True)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    out.to_parquet(OUT_PATH, index=False)
    print(f"\nWrote {len(out)} rows to {OUT_PATH}")
    print(f"Columns: {list(out.columns)}")
