"""
Pull Open-Meteo Ensemble forecast for the next 3 days at the user's home
location. Uses GFS Ensemble (31 members, 25km, available globally) for
probabilistic forecasts with explicit ensemble member granularity.

Default location is the centroid of the user's run starting points
(pulled from mart_run_samples). Override with HOME_LAT/HOME_LON env vars
if you want a fixed home location.

Outputs: data/raw/forecast_ensemble.parquet, long format
(forecast_time, member, variable, value).
"""

import os
import sys
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector

PLATFORM = os.path.expanduser("~/work/personal/health-analytics-platform")
sys.path.insert(0, PLATFORM)
from ingestion.common.snowflake_auth import load_private_key

load_dotenv(os.path.join(PLATFORM, ".env"))

OUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "forecast_ensemble.parquet")

HOME_LAT = os.getenv("HOME_LAT")
HOME_LON = os.getenv("HOME_LON")

HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "wind_speed_10m",
    "precipitation",
]


def get_default_location():
    """Centroid of the user's actual run starting points."""
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
        SELECT AVG(first_lat), AVG(first_lon)
        FROM (
            SELECT DISTINCT activity_id, first_lat, first_lon
            FROM mart_run_samples
        )
    """)
    lat, lon = cur.fetchone()
    cur.close()
    conn.close()
    return float(lat), float(lon)


def fetch_ensemble(lat, lon):
    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY_VARS),
        "models": "gfs_seamless",
        "forecast_days": 3,
        "timezone": "America/Chicago",
    }
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def reshape_to_long(payload):
    """Flatten the ensemble response into a long-format DataFrame.
    Open-Meteo returns each variable's data as a list of arrays per member,
    or as suffixed keys like temperature_2m_member01. We handle both."""
    times = payload["hourly"]["time"]
    rows = []
    for key, values in payload["hourly"].items():
        if key == "time":
            continue
        # Variable + member parsing — keys come as 'temperature_2m' (mean/control)
        # or 'temperature_2m_member01', 'temperature_2m_member02', etc.
        if "_member" in key:
            variable, member_part = key.rsplit("_member", 1)
            member = int(member_part)
        else:
            variable = key
            member = 0  # control / deterministic member
        for ts, v in zip(times, values):
            rows.append({
                "forecast_time": ts,
                "variable": variable,
                "member": member,
                "value": v,
            })
    df = pd.DataFrame(rows)
    df["forecast_time"] = pd.to_datetime(df["forecast_time"])
    return df


if __name__ == "__main__":
    if HOME_LAT and HOME_LON:
        lat, lon = float(HOME_LAT), float(HOME_LON)
        print(f"Using configured home location: ({lat:.4f}, {lon:.4f})")
    else:
        lat, lon = get_default_location()
        print(f"Using run-centroid location: ({lat:.4f}, {lon:.4f})")

    print(f"Fetching 3-day GFS Ensemble forecast...")
    payload = fetch_ensemble(lat, lon)
    df = reshape_to_long(payload)

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)

    n_members = df["member"].nunique()
    n_vars = df["variable"].nunique()
    n_hours = df["forecast_time"].nunique()
    print(f"Wrote {len(df)} rows to {OUT_PATH}")
    print(f"  {n_hours} forecast hours × {n_vars} variables × {n_members} ensemble members")
    print(f"  Time range: {df['forecast_time'].min()} → {df['forecast_time'].max()}")
