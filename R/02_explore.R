# Quick visual sanity checks before any modeling.
suppressPackageStartupMessages({
  library(tidyverse)
  library(arrow)
  library(here)
})

d <- read_parquet(here("data", "processed", "run_features.parquet"))

# 1. HR vs date, colored by temperature
p1 <- ggplot(d, aes(x = local_date, y = mean_hr, color = mean_temp_c, size = distance_km)) +
  geom_point() +
  scale_color_gradient(low = "blue", high = "red", name = "Temp (C)") +
  labs(title = "Mean HR per run over time, by temperature",
       x = "Date", y = "Mean HR (bpm)") +
  theme_minimal()
ggsave(here("output", "figures", "01_hr_over_time.png"), p1, width = 9, height = 5, dpi = 120)

# 2. HR-at-pace vs cumulative heat exposure
p2 <- ggplot(d %>% filter(!is.na(mean_pace_min_per_km), mean_pace_min_per_km < 12),
             aes(x = cumulative_heat_exposure_cd, y = mean_hr, color = mean_pace_min_per_km)) +
  geom_point(size = 3) +
  geom_smooth(method = "loess", se = TRUE, color = "black", alpha = 0.2) +
  scale_color_viridis_c(name = "Pace\n(min/km)", direction = -1) +
  labs(title = "Mean HR vs cumulative heat exposure",
       subtitle = "If acclimation is occurring, slope should be negative net of pace",
       x = "Cumulative heat exposure (C-days above 15C)", y = "Mean HR (bpm)") +
  theme_minimal()
ggsave(here("output", "figures", "02_hr_vs_heat_exposure.png"), p2, width = 9, height = 5, dpi = 120)

# 3. Temperature on run days
p3 <- ggplot(d, aes(x = local_date, y = mean_temp_c)) +
  geom_col(fill = "tomato", alpha = 0.7) +
  geom_hline(yintercept = 15, linetype = "dashed") +
  labs(title = "Ambient temperature during runs",
       subtitle = "Dashed line = 15C heat-acclimation threshold",
       x = "Date", y = "Mean ambient temp during run (C)") +
  theme_minimal()
ggsave(here("output", "figures", "03_temp_per_run.png"), p3, width = 9, height = 5, dpi = 120)

cat("Saved 3 plots to output/figures/\n")
cat("\n=== Summary stats ===\n")
print(d %>% summarise(
  n_runs                = n(),
  mean_hr_bpm           = round(mean(mean_hr, na.rm = TRUE), 1),
  mean_pace_min_per_km  = round(mean(mean_pace_min_per_km, na.rm = TRUE), 2),
  mean_temp_c           = round(mean(mean_temp_c, na.rm = TRUE), 1),
  total_distance_km     = round(sum(distance_km, na.rm = TRUE), 1),
  total_heat_exposure_cd = round(max(cumulative_heat_exposure_cd, na.rm = TRUE), 1)
))
