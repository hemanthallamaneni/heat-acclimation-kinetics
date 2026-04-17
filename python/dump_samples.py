"""Dump mart_run_samples from Snowflake to data/raw/run_samples.parquet
so the R analysis can read it without a database driver."""
import os
import sys
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector

PLATFORM_PATH = os.path.expanduser("~/work/personal/health-analytics-platform")
sys.path.insert(0, PLATFORM_PATH)
from ingestion.common.snowflake_auth import load_private_key

load_dotenv(os.path.join(PLATFORM_PATH, ".env"))

OUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "run_samples.parquet")

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    private_key=load_private_key(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema="MART_HEALTH",
)
cur = conn.cursor()
cur.execute("SELECT * FROM mart_run_samples")
cols = [c[0].lower() for c in cur.description]
df = pd.DataFrame(cur.fetchall(), columns=cols)
cur.close()
conn.close()

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
df.to_parquet(OUT_PATH, index=False)
print(f"Wrote {len(df)} rows to {OUT_PATH}")
