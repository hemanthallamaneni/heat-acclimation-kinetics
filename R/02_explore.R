# Visual sanity checks before modeling.
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

# 2. HR vs cumulative heat exposure, colored by pace
p2 <- ggplot(d %>% filter(!is.na(mean_pace_min_per_km), mean_pace_min_per_km < 12),
             aes(x = cumulative_heat_exposure_cd, y = mean_hr, color = mean_pace_min_per_km)) +
  geom_point(size = 3) +
  geom_smooth(method = "loess", se = TRUE, color = "black", alpha = 0.2) +
  scale_color_viridis_c(name = "Pace\n(min/km)", direction = -1) +
  labs(title = "Mean HR vs cumulative heat exposure",
       subtitle = "Naive bivariate — does not control for pace or training load",
       x = "Cumulative heat exposure (C-days above 15C)", y = "Mean HR (bpm)") +
  theme_minimal()
ggsave(here("output", "figures", "02_hr_vs_heat_exposure.png"), p2, width = 9, height = 5, dpi = 120)

# 3. Temperature on run days (FIXED: use point, not geom_col which stacks)
p3 <- ggplot(d, aes(x = local_date, y = mean_temp_c)) +
  geom_segment(aes(xend = local_date, yend = 0), color = "tomato", alpha = 0.4) +
  geom_point(color = "tomato", size = 2.5) +
  geom_hline(yintercept = 15, linetype = "dashed") +
  labs(title = "Ambient temperature during runs",
       subtitle = "Dashed line = 15C heat-acclimation threshold; lollipop = one run",
       x = "Date", y = "Mean ambient temp during run (C)") +
  theme_minimal() +
  ylim(0, 35)
ggsave(here("output", "figures", "03_temp_per_run.png"), p3, width = 9, height = 5, dpi = 120)

# 4. NEW: HR-at-pace decomposition — bin runs by pace, plot HR over time within each
p4 <- d %>%
  filter(!is.na(mean_pace_min_per_km), mean_pace_min_per_km < 12) %>%
  mutate(
    pace_bin = cut(mean_pace_min_per_km,
                   breaks = c(0, 5.5, 6.5, 12),
                   labels = c("Faster (<5:30/km)", "Moderate (5:30-6:30/km)", "Easier (>6:30/km)"))
  ) %>%
  ggplot(aes(x = local_date, y = mean_hr, color = mean_temp_c)) +
    geom_point(size = 3) +
    geom_smooth(method = "loess", se = FALSE, color = "black", alpha = 0.5) +
    facet_wrap(~ pace_bin, scales = "fixed") +
    scale_color_gradient(low = "blue", high = "red", name = "Temp (C)") +
    labs(title = "HR over time, by pace bin",
         subtitle = "Within a pace bin, HR drift over time should reflect heat + training load",
         x = "Date", y = "Mean HR (bpm)") +
    theme_minimal()
ggsave(here("output", "figures", "04_hr_by_pace_bin.png"), p4, width = 12, height = 5, dpi = 120)

# 5. NEW: distance vs HR — confirms training load confound
p5 <- ggplot(d, aes(x = distance_km, y = mean_hr, color = mean_temp_c)) +
  geom_point(size = 3) +
  geom_smooth(method = "lm", color = "black", alpha = 0.2) +
  scale_color_gradient(low = "blue", high = "red", name = "Temp (C)") +
  labs(title = "Mean HR vs run distance",
       subtitle = "Longer runs may pull HR up via cardiac drift independently of heat",
       x = "Distance (km)", y = "Mean HR (bpm)") +
  theme_minimal()
ggsave(here("output", "figures", "05_hr_vs_distance.png"), p5, width = 9, height = 5, dpi = 120)

cat("Saved 5 plots to output/figures/\n")
