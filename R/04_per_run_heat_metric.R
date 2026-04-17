# Per-run heat exposure metric: integrate (ambient_temp - threshold) × duration
# over each run individually, using the within-run weather data for that day.
# Then re-fit NLME with this less-collinear cumulative measure.
#
# Why this might break the days↔heat collinearity:
# Some early March runs were 24°C (high heat-load per run despite early calendar)
# Some mid-March runs were 10°C (low heat-load despite later calendar)
# The new cumulative metric will track actual heat exposure, not calendar.

suppressPackageStartupMessages({
  library(tidyverse); library(arrow); library(here); library(nlme); library(lubridate)
})

samples <- read_parquet(here("data", "raw", "run_samples.parquet"))
weather <- read_parquet(here("data", "raw", "weather.parquet"))
features <- read_parquet(here("data", "processed", "run_features.parquet"))

names(samples) <- tolower(names(samples)); names(weather) <- tolower(names(weather))
weather <- weather %>%
  mutate(weather_ts_local = ymd_hm(time, tz = "America/Chicago"),
         weather_ts_utc   = with_tz(weather_ts_local, tzone = "UTC"))

THRESHOLD_C <- 15

# For each run, compute total heat-load using mean run temp × duration in hours
per_run_heat <- features %>%
  mutate(
    heat_load_run = pmax(mean_temp_c - THRESHOLD_C, 0) * (duration_min / 60)
  ) %>%
  arrange(local_date) %>%
  mutate(
    cum_heat_per_run = cumsum(heat_load_run)
  ) %>%
  select(activity_id, heat_load_run, cum_heat_per_run)

d <- features %>%
  left_join(per_run_heat, by = "activity_id") %>%
  filter(!is.na(mean_pace_min_per_km), mean_pace_min_per_km < 12) %>%
  mutate(
    pace_centered     = mean_pace_min_per_km - mean(mean_pace_min_per_km),
    dist_centered     = distance_km - mean(distance_km),
    days_centered     = days_since_start - mean(days_since_start),
    heat_v1_centered  = cumulative_heat_exposure_cd - mean(cumulative_heat_exposure_cd),
    heat_v2_centered  = cum_heat_per_run - mean(cum_heat_per_run)
  ) %>%
  arrange(start_date_utc)

cat("=== Collinearity check ===\n")
cat(sprintf("cor(days, heat_v1 [calendar-based]):  %.3f\n",
            cor(d$days_centered, d$heat_v1_centered)))
cat(sprintf("cor(days, heat_v2 [per-run-based]):    %.3f\n",
            cor(d$days_centered, d$heat_v2_centered)))
cat("\n")

cat("=== Range comparison ===\n")
cat(sprintf("v1 range: %.1f to %.1f C-days (calendar accumulation)\n",
            min(d$cumulative_heat_exposure_cd), max(d$cumulative_heat_exposure_cd)))
cat(sprintf("v2 range: %.1f to %.1f C-hours (per-run accumulation)\n",
            min(d$cum_heat_per_run), max(d$cum_heat_per_run)))
cat("\n")

# --- Refit with v2 heat metric ---
m_ar1_v2 <- gls(
  mean_hr ~ pace_centered + dist_centered + days_centered + heat_v2_centered,
  data = d,
  correlation = corAR1(form = ~ 1),
  method = "REML"
)

cat("=== Model 3: same spec but with per-run heat metric ===\n")
print(summary(m_ar1_v2))
cat("\n=== 95% CIs ===\n")
print(intervals(m_ar1_v2, which = "coef"))

cat("\n=== Interpretation ===\n")
coefs <- coef(m_ar1_v2)
cat(sprintf("Per 1 C-hour cumulative per-run heat: %+.3f bpm\n", coefs["heat_v2_centered"]))
cat(sprintf("Per 1 day later in block:             %+.2f bpm\n", coefs["days_centered"]))

# Save
write_parquet(d, here("data", "processed", "run_features_v2.parquet"))
saveRDS(m_ar1_v2, here("data", "processed", "m_ar1_v2.rds"))
