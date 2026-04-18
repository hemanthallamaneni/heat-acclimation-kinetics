"""Dump Oura sleep + readiness joined by date from Snowflake to local parquet
for R consumption. Reads from the staging models in MART_HEALTH."""

import os
import sys
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector

PLATFORM = os.path.expanduser("~/work/personal/health-analytics-platform")
sys.path.insert(0, PLATFORM)
from ingestion.common.snowflake_auth import load_private_key

load_dotenv(os.path.join(PLATFORM, ".env"))

OUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "oura_daily.parquet")

QUERY = """
SELECT
    s.sleep_date                       AS oura_date,
    s.average_hrv,
    s.lowest_heart_rate                AS oura_rhr,
    s.average_heart_rate               AS oura_avg_hr,
    s.efficiency                       AS sleep_efficiency,
    s.total_sleep_duration             AS sleep_total_sec,
    s.deep_sleep_duration              AS sleep_deep_sec,
    s.rem_sleep_duration               AS sleep_rem_sec,
    s.light_sleep_duration             AS sleep_light_sec,
    s.awake_time                       AS sleep_awake_sec,
    s.sleep_latency                    AS sleep_latency_sec,
    s.average_breath                   AS oura_breath_rate,
    r.readiness_score,
    r.temperature_deviation            AS oura_temp_dev,
    r.temperature_trend_deviation      AS oura_temp_trend_dev,
    r.contrib_activity_balance,
    r.contrib_body_temperature,
    r.contrib_hrv_balance,
    r.contrib_recovery_index,
    r.contrib_resting_heart_rate       AS contrib_rhr_score,
    r.contrib_sleep_balance
FROM stg_oura_sleep s
LEFT JOIN stg_oura_readiness r ON s.sleep_date = r.readiness_date
WHERE s.sleep_type = 'long_sleep'
ORDER BY s.sleep_date
"""

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    private_key=load_private_key(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema="MART_HEALTH",
)
cur = conn.cursor()
cur.execute(QUERY)
cols = [c[0].lower() for c in cur.description]
df = pd.DataFrame(cur.fetchall(), columns=cols)
cur.close()
conn.close()

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
df.to_parquet(OUT_PATH, index=False)
print(f"Wrote {len(df)} rows to {OUT_PATH}")
print(f"Date range: {df['oura_date'].min()} to {df['oura_date'].max()}")
print(f"Readiness coverage: {df['readiness_score'].notna().sum()} of {len(df)} days")
