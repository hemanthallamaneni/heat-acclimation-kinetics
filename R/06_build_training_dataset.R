# Build the training dataset for the forecast model.
#
# Joins per-run telemetry (Strava streams), per-day Oura sleep + readiness,
# and historical weather. Computes:
#   * Targets: (b) Z2-ceiling pace, (c) GOVSS-style stress proxy,
#              (d) recovery-adjusted composite (placeholder, refined Saturday)
#   * Recovery features: prior-night Oura raw inputs (HRV, RHR, temp deviation,
#                        sleep stages)
#   * Training-load features: rolling 7d/28d distance and duration, ACWR,
#                              days since last hard effort
#   * Weather features: per-run mean ambient temp, humidity, dewpoint,
#                       apparent temp (mean during the run window)
#   * Comparison baseline: Oura readiness score (single-input baseline model)
#
# Output: data/processed/training_features.parquet

suppressPackageStartupMessages({
  library(tidyverse); library(arrow); library(here); library(lubridate)
})

# ---- Load sources ----
samples  <- read_parquet(here("data", "raw", "run_samples.parquet"))
weather  <- read_parquet(here("data", "raw", "weather.parquet"))
oura     <- read_parquet(here("data", "raw", "oura_daily.parquet"))

names(samples) <- tolower(names(samples))
names(weather) <- tolower(names(weather))
names(oura)    <- tolower(names(oura))

cat("Loaded", n_distinct(samples$activity_id), "runs,",
    nrow(weather), "weather rows,", nrow(oura), "Oura day records\n\n")

# ---- Per-run aggregates ----
# Strip first/last 5% of run by sample index (warmup/cooldown), keep moving samples
run_core <- samples %>%
  group_by(activity_id) %>%
  arrange(sample_index) %>%
  mutate(
    pct = sample_index / max(sample_index)
  ) %>%
  filter(pct >= 0.05, pct <= 0.95) %>%
  ungroup()

run_features <- run_core %>%
  group_by(activity_id, local_date, hr_source, first_lat, first_lon,
           distance_meters, elapsed_time, start_date) %>%
  summarise(
    n_core_samples       = n(),
    median_velocity_mps  = median(velocity_mps, na.rm = TRUE),
    mean_velocity_mps    = mean(velocity_mps, na.rm = TRUE),
    median_grade_pct     = median(grade_pct, na.rm = TRUE),
    total_elev_gain_m    = sum(pmax(diff(altitude_m), 0), na.rm = TRUE),
    median_cadence_spm   = median(cadence_spm, na.rm = TRUE),
    .groups              = "drop"
  ) %>%
  mutate(
    distance_km           = distance_meters / 1000,
    duration_min          = elapsed_time / 60,
    median_pace_min_per_km = (1000 / median_velocity_mps) / 60,
    # Grade-adjusted pace: simple linear correction (~0.03 min/km per % grade)
    # Riegel-style adjustment is more accurate but linear works for our range
    grade_adj_pace = median_pace_min_per_km - 0.03 * median_grade_pct,
    start_date_utc        = ymd_hms(start_date, tz = "UTC"),
    local_date            = as_date(local_date)
  )

# ---- Targets ----
# (b) Z2 ceiling pace: grade-adjusted median pace, treating each Z2-flagged
#     run as a sample of sustainable Z2 pace ceiling. We don't know which
#     runs are explicitly Z2 from the data, so we use all easy/aerobic runs:
#     pace > 5 min/km (excludes intervals/tempos which aren't Z2)
run_features <- run_features %>%
  mutate(
    target_b_z2_pace = if_else(median_pace_min_per_km > 5.0, grade_adj_pace, NA_real_)
  )

# (c) GOVSS-style stress proxy: integrate (grade-adjusted-velocity ^ 3) over time.
#     The cube exponent comes from the energetics literature (cost of running
#     scales nonlinearly with pace above easy threshold). Normalized to per-km.
run_features <- run_features %>%
  mutate(
    grade_adj_velocity_mps = 1000 / (grade_adj_pace * 60),
    target_c_stress_per_km = (grade_adj_velocity_mps ^ 3) * duration_min / distance_km
  )

# (d) Composite: placeholder, will refine Saturday after seeing the data
#     For now, normalized stress with placeholder weight
run_features <- run_features %>%
  mutate(
    target_d_composite = target_c_stress_per_km
  )

# ---- Weather per run ----
weather <- weather %>%
  mutate(
    weather_ts_local = ymd_hm(time, tz = "America/Chicago"),
    weather_ts_utc   = with_tz(weather_ts_local, tzone = "UTC")
  )

weather_per_run <- run_features %>%
  select(activity_id, start_date_utc, duration_min, first_lat, first_lon) %>%
  mutate(run_end_utc = start_date_utc + minutes(round(duration_min))) %>%
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
    mean_temp_c          = map_dbl(weather_window, ~ mean(.x$temperature_2m, na.rm = TRUE)),
    max_temp_c           = map_dbl(weather_window, ~ max(.x$temperature_2m, na.rm = TRUE)),
    mean_apparent_temp_c = map_dbl(weather_window, ~ mean(.x$apparent_temperature, na.rm = TRUE)),
    mean_humidity_pct    = map_dbl(weather_window, ~ mean(.x$relative_humidity_2m, na.rm = TRUE)),
    mean_dewpoint_c      = map_dbl(weather_window, ~ mean(.x$dew_point_2m, na.rm = TRUE)),
    mean_wind_mps        = map_dbl(weather_window, ~ mean(.x$wind_speed_10m, na.rm = TRUE))
  ) %>%
  select(-weather_window, -start_date_utc, -duration_min, -first_lat, -first_lon, -run_end_utc)

# ---- Recovery state: prior-night Oura ----
# Oura sleep_date is the morning the night ended. So a run on date D
# uses Oura record where sleep_date = D (the night before).
oura_per_run <- run_features %>%
  select(activity_id, local_date) %>%
  left_join(oura, by = c("local_date" = "oura_date"))

# ---- Training load features ----
# Rolling sums of distance and duration in 7d and 28d windows, plus ACWR.
run_features_arranged <- run_features %>%
  arrange(local_date) %>%
  mutate(
    days_since_start = as.integer(local_date - min(local_date))
  )

training_load <- run_features_arranged %>%
  select(activity_id, local_date, distance_km, duration_min, target_c_stress_per_km) %>%
  rowwise() %>%
  mutate(
    # For each run, compute load over preceding windows (excluding current run)
    rolling_7d_distance = sum(run_features_arranged$distance_km[
      run_features_arranged$local_date < local_date &
      run_features_arranged$local_date >= local_date - days(7)
    ], na.rm = TRUE),
    rolling_28d_distance = sum(run_features_arranged$distance_km[
      run_features_arranged$local_date < local_date &
      run_features_arranged$local_date >= local_date - days(28)
    ], na.rm = TRUE),
    rolling_7d_stress = sum(run_features_arranged$target_c_stress_per_km[
      run_features_arranged$local_date < local_date &
      run_features_arranged$local_date >= local_date - days(7)
    ], na.rm = TRUE),
    rolling_28d_stress = sum(run_features_arranged$target_c_stress_per_km[
      run_features_arranged$local_date < local_date &
      run_features_arranged$local_date >= local_date - days(28)
    ], na.rm = TRUE)
  ) %>%
  ungroup() %>%
  mutate(
    acwr_distance = if_else(rolling_28d_distance > 0,
                            (rolling_7d_distance / 7) / (rolling_28d_distance / 28),
                            NA_real_),
    acwr_stress   = if_else(rolling_28d_stress > 0,
                            (rolling_7d_stress / 7) / (rolling_28d_stress / 28),
                            NA_real_)
  ) %>%
  select(activity_id, rolling_7d_distance, rolling_28d_distance,
         rolling_7d_stress, rolling_28d_stress, acwr_distance, acwr_stress)

# ---- Final join ----
training_features <- run_features_arranged %>%
  left_join(weather_per_run, by = "activity_id") %>%
  left_join(oura_per_run %>% select(-local_date), by = "activity_id") %>%
  left_join(training_load, by = "activity_id") %>%
  arrange(local_date)

# ---- Save ----
dir.create(here("data", "processed"), showWarnings = FALSE, recursive = TRUE)
write_parquet(training_features, here("data", "processed", "training_features.parquet"))

cat("\n=== Training features built ===\n")
cat("Rows:", nrow(training_features), "\n")
cat("Columns:", ncol(training_features), "\n")
cat("Date range:", as.character(min(training_features$local_date)),
    "to", as.character(max(training_features$local_date)), "\n\n")

cat("=== Coverage of key inputs ===\n")
key_cols <- c("target_b_z2_pace", "target_c_stress_per_km", "target_d_composite",
              "mean_temp_c", "average_hrv", "oura_rhr", "readiness_score",
              "oura_temp_dev", "rolling_7d_distance", "acwr_distance")
for (col in key_cols) {
  if (col %in% names(training_features)) {
    n_present <- sum(!is.na(training_features[[col]]))
    cat(sprintf("  %-30s %d/%d non-null\n", col, n_present, nrow(training_features)))
  }
}

cat("\n=== First rows (key columns) ===\n")
print(training_features %>% select(local_date, target_b_z2_pace, target_c_stress_per_km,
                                    mean_temp_c, average_hrv, oura_rhr, readiness_score,
                                    rolling_7d_distance, acwr_distance) %>% head(10))
