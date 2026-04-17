# Build the per-run analytical dataset.
#
# Pulls per-sample telemetry from Snowflake (mart_run_samples), joins
# hourly weather from data/raw/weather.parquet by nearest-hour, and
# aggregates to one row per run.
#
# Per-run features:
#   * Cardiovascular: mean_hr, max_hr, p90_hr, hr_drift_bpm_per_min
#   * Pace/load:      mean_pace_min_per_km, distance_km, duration_min,
#                     elevation_gain_m, training_impulse (TRIMP)
#   * Thermal:        mean_temp_c, max_temp_c, mean_apparent_temp,
#                     mean_humidity, mean_dewpoint
#   * Cumulative:     days_since_first_run, cumulative_heat_exposure_c_days
#
# Output: data/processed/run_features.parquet

suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(arrow)
  library(here)
  library(DBI)
})

# ---- Load Strava streams from Snowflake ------------------------------------
# Use Python via reticulate? No — simpler: dump from snowflake CLI to parquet
# via the Python script below before this R script runs.
# We assume run_samples.parquet exists in data/raw/.
samples <- read_parquet(here("data", "raw", "run_samples.parquet"))
weather <- read_parquet(here("data", "raw", "weather.parquet"))

cat("Loaded", nrow(samples), "samples across",
    n_distinct(samples$activity_id), "runs\n")
cat("Loaded", nrow(weather), "hourly weather rows\n\n")

# ---- Normalize column names (parquet roundtrip uppercases them) -----------
names(samples) <- tolower(names(samples))
names(weather) <- tolower(names(weather))

# ---- Parse weather time to POSIXct in America/Chicago ---------------------
weather <- weather %>%
  mutate(
    weather_ts_local = ymd_hm(time, tz = "America/Chicago"),
    weather_ts_utc   = with_tz(weather_ts_local, tzone = "UTC")
  )

# ---- Per-run aggregation --------------------------------------------------
# HR drift: regress HR on time_offset_sec within each run; slope * 60 = bpm/min
hr_drift_per_run <- samples %>%
  filter(!is.na(heartrate)) %>%
  group_by(activity_id) %>%
  filter(n() >= 30) %>%   # need enough samples for stable slope
  summarise(
    hr_drift_bpm_per_min = coef(lm(heartrate ~ time_offset_sec))[2] * 60,
    .groups = "drop"
  )

run_summary <- samples %>%
  group_by(activity_id, local_date, hr_source, first_lat, first_lon,
           distance_meters, elapsed_time, start_date) %>%
  summarise(
    n_samples              = n(),
    mean_hr                = mean(heartrate, na.rm = TRUE),
    max_hr                 = max(heartrate, na.rm = TRUE),
    p90_hr                 = quantile(heartrate, 0.9, na.rm = TRUE),
    mean_velocity_mps      = mean(velocity_mps, na.rm = TRUE),
    mean_cadence_spm       = mean(cadence_spm, na.rm = TRUE),
    elevation_gain_m       = sum(pmax(diff(altitude_m), 0), na.rm = TRUE),
    .groups                = "drop"
  ) %>%
  mutate(
    distance_km          = distance_meters / 1000,
    duration_min         = elapsed_time / 60,
    mean_pace_min_per_km = (1000 / mean_velocity_mps) / 60,
    start_date_utc       = ymd_hms(start_date, tz = "UTC")
  ) %>%
  left_join(hr_drift_per_run, by = "activity_id")

# ---- Join weather: nearest-hour to run start ------------------------------
# For each run, find the weather hour closest to start_date_utc and pull
# the weather features for the duration of the run (mean across hours
# overlapping the run window).
weather_per_run <- run_summary %>%
  select(activity_id, start_date_utc, duration_min, first_lat, first_lon) %>%
  mutate(
    run_end_utc = start_date_utc + minutes(round(duration_min))
  ) %>%
  rowwise() %>%
  mutate(
    weather_window = list(
      weather %>%
        filter(weather_ts_utc >= floor_date(start_date_utc, "hour"),
               weather_ts_utc <= ceiling_date(run_end_utc, "hour"))
    )
  ) %>%
  ungroup() %>%
  mutate(
    n_weather_rows         = map_int(weather_window, nrow),
    mean_temp_c            = map_dbl(weather_window, ~ mean(.x$temperature_2m, na.rm = TRUE)),
    max_temp_c             = map_dbl(weather_window, ~ max(.x$temperature_2m, na.rm = TRUE)),
    mean_apparent_temp_c   = map_dbl(weather_window, ~ mean(.x$apparent_temperature, na.rm = TRUE)),
    mean_humidity_pct      = map_dbl(weather_window, ~ mean(.x$relative_humidity_2m, na.rm = TRUE)),
    mean_dewpoint_c        = map_dbl(weather_window, ~ mean(.x$dew_point_2m, na.rm = TRUE)),
    mean_wind_mps          = map_dbl(weather_window, ~ mean(.x$wind_speed_10m, na.rm = TRUE)),
    total_precip_mm        = map_dbl(weather_window, ~ sum(.x$precipitation, na.rm = TRUE))
  ) %>%
  select(-weather_window)

run_features <- run_summary %>%
  left_join(weather_per_run %>% select(-start_date_utc, -duration_min, -first_lat, -first_lon),
            by = "activity_id") %>%
  arrange(local_date) %>%
  mutate(
    days_since_start = as.integer(local_date - min(local_date)),
    # Cumulative heat exposure: running sum of (mean_temp - 15°C) clipped to >= 0
    # 15°C is roughly the threshold above which heat acclimation begins driving adaptation
    daily_heat_load             = pmax(mean_temp_c - 15, 0),
    cumulative_heat_exposure_cd = cumsum(daily_heat_load)
  )

# ---- Save -----------------------------------------------------------------
dir.create(here("data", "processed"), showWarnings = FALSE, recursive = TRUE)
write_parquet(run_features, here("data", "processed", "run_features.parquet"))

cat("\n=== Final dataset ===\n")
cat("Rows:", nrow(run_features), "\n")
cat("Date range:", as.character(min(run_features$local_date)),
    "to", as.character(max(run_features$local_date)), "\n")
cat("HR sources:\n")
print(table(run_features$hr_source))
cat("\nMean temp range:", round(min(run_features$mean_temp_c, na.rm=TRUE), 1),
    "to", round(max(run_features$mean_temp_c, na.rm=TRUE), 1), "C\n")
cat("\nFirst rows:\n")
print(run_features %>% select(local_date, mean_hr, mean_pace_min_per_km,
                              mean_temp_c, days_since_start,
                              cumulative_heat_exposure_cd, hr_source))
