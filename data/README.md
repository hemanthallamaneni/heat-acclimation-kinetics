# Data regeneration

`data/raw/` and `data/processed/` are gitignored — the contents depend on your Snowflake instance and on live API pulls. To regenerate:

1. Ensure the [Health Analytics Platform](https://github.com/hemanthallamaneni/health-analytics-platform) is set up, populated, and `.env` configured. The analysis pulls from your Snowflake instance.

2. Pull source data into local parquet (one-time, then on-demand for refresh):
```bash
   source .venv/bin/activate
   python python/dump_samples.py        # mart_run_samples → data/raw/run_samples.parquet
   python python/dump_oura_features.py  # sleep + readiness → data/raw/oura_daily.parquet
   python python/fetch_weather.py       # historical weather per run → data/raw/weather.parquet
   python python/fetch_forecast.py      # 3-day forecast ensemble → data/raw/forecast_ensemble.parquet
```

3. Build the analytical dataset:
```bash
   Rscript R/01_build_dataset.R              # heat-era per-run features (legacy, still useful)
   Rscript R/06_build_training_dataset.R     # current forecast model training data
```

The two-stage approach (raw pull → R-side feature build) exists because R's Snowflake driver is heavier than Python's; pulling once to parquet and reading from parquet in R is faster and more reproducible.
