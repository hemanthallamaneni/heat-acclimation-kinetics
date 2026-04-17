# Heat Acclimation Kinetics from Personal Endurance Telemetry

Field-based estimation of heat acclimation kinetics using nonlinear mixed-effects modeling on N=1 longitudinal endurance data from a Dallas marathon training block (late February through April 2026).

## Status

Active research project. Paper in progress.

## Dependencies

This analysis depends on data produced by the [Health Analytics Platform](https://github.com/hemanthallamaneni/health-analytics-platform), which ingests Strava, Oura, Apple Health, and RENPHO data into Snowflake. Specifically requires:

- Per-second Strava activity streams (HR, cadence, velocity, GPS) in `staging.stg_strava_streams`
- Daily summary mart (`mart_health.daily_health_summary`)
- Externally joined Open-Meteo historical weather data

## Stack

- R (analysis): `nlme`, `dplyr`, `ggplot2`, `lubridate`, `splines`
- Python (data access): `snowflake-connector-python`
- LaTeX (paper)
- Snowflake (data warehouse, via the platform repo above)

## Repo structure

\`\`\`
R/                    Analysis scripts
data/raw/             Raw extracts (gitignored)
data/processed/       Analytic dataset (gitignored)
output/figures/       Generated figures (gitignored)
output/tables/        Generated tables (gitignored)
paper/                LaTeX source
\`\`\`

## Methodological positioning

The data structures underlying personal physiological telemetry are isomorphic to those in claims, EHR, and operational healthcare data — longitudinal records keyed by entity and time, joined across heterogeneous sources, with quality issues. The methods demonstrated here transfer directly to those settings; the substrate is what the author has direct access to.
