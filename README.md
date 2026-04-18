# Training Envelope

A personal training capacity forecasting framework: predict the **band of plausible training quality** for upcoming days as a function of forecast weather (with ensemble uncertainty), recovery state from overnight physiology, and recent training load.

## What this is

Consumer wearables collapse recovery into a single number per day (Oura readiness, Whoop strain, etc.). This project preserves the structure those scores discard by producing **probabilistic forecasts with calibrated uncertainty bands** — letting the user see not just "today's expected effort cost" but the full plausible range, conditional on each input source's own uncertainty.

The methodological signature is the same as in [Physiological Nonstationarity](https://github.com/hemanthallamaneni/health-analytics-platform): standard analytical defaults silently throw away predictive structure that finer modeling can recover.

## Inputs

- **Weather forecast** with ensemble uncertainty (Open-Meteo GFS Ensemble, 31 members, 3-day horizon, 25km resolution)
- **Recovery state** from raw Oura overnight metrics (HRV, RHR, skin temperature deviation, sleep architecture) — *not* the proprietary readiness score, which becomes the baseline comparison
- **Recent training load** from Strava (rolling 7d/28d distance and duration, days since last hard effort)

## Targets

Operationalized as pace-derived metrics rather than HR-derived to sidestep optical sensor failure modes documented elsewhere in this work:

- **(b)** Sustainable Z2 ceiling pace — median grade-adjusted pace per run, warmup/cooldown excluded
- **(c)** Pace-and-grade-based stress proxy (GOVSS-style) — physiological cost integral without trusting per-second HR
- **(d)** Composite recovery-adjusted capacity — weighted combination of (b) and (c) by current recovery state

## Methodology

Quantile regression (R `quantreg::rq`) predicting 10th, 50th, 90th percentile training capacity from the input set. Cross-validated on temporal splits. Calibration assessed against held-out runs.

The headline comparison: does this multi-input quantile model outperform a single-input baseline using only the proprietary Oura readiness score? If yes (we expect so), the paper's claim is that **single-number recovery scoring discards predictive information that's recoverable with principled multi-source modeling**.

## Project history

This repo originated as a heat acclimation kinetics study (`R/03_fit_nlme.R`, `R/04_per_run_heat_metric.R`, `R/05_robustness.R`). That analysis surfaced a structural identifiability problem — heat exposure and time-in-block were too collinear at the available sample size to separate causally — which motivated the pivot to the present forecasting framing, where the same data is used predictively rather than for causal inference. The heat scripts remain as the analytical foundation that informed the current direction.

## Dependencies

This analysis depends on data produced by the [Health Analytics Platform](https://github.com/hemanthallamaneni/health-analytics-platform), which ingests Strava streams, Oura sleep + readiness, Apple Health, and RENPHO into Snowflake. Specifically requires:

- `MART_HEALTH.mart_run_samples` — per-second telemetry for analyzable runs
- `MART_HEALTH.stg_oura_sleep` — overnight HRV, RHR, sleep architecture
- `MART_HEALTH.stg_oura_readiness` — temperature deviation, readiness contributors
- Local cache of Open-Meteo historical archive + ensemble forecast pulls

## Stack

- R: `quantreg`, `nlme`, `tidyverse`, `lubridate`, `arrow`
- Python: `snowflake-connector-python`, `pandas`, `pyarrow`, `requests`
- LaTeX (paper)

## Repo structure
## Methodological positioning

The data structures underlying personal physiological telemetry are isomorphic to those in claims, EHR, and operational healthcare data — longitudinal records keyed by entity and time, joined across heterogeneous sources, with quality issues. The methods demonstrated here transfer directly to those settings; the substrate is what the author has direct access to.
