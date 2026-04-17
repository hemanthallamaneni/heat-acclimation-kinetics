# Mixed-effects fit of mean HR per run as a function of pace, distance,
# days_since_start (training adaptation proxy), and cumulative_heat_exposure.
#
# Model:
#   mean_hr ~ b0 + b_pace * pace + b_dist * distance + b_train * days
#           + b_heat * cumulative_heat + e
#
# This is a fixed-effects-only specification first because there is no
# obvious grouping factor at n=30 — every run is its own observation
# from a single subject. Residuals will be inspected for autocorrelation
# (consecutive runs may not be independent due to short-term fatigue
# carryover); if present, we'll add a gls() with corAR1.

suppressPackageStartupMessages({
  library(tidyverse)
  library(arrow)
  library(here)
  library(nlme)
  library(broom)
})

d <- read_parquet(here("data", "processed", "run_features.parquet")) %>%
  filter(!is.na(mean_pace_min_per_km), mean_pace_min_per_km < 12) %>%
  mutate(
    pace_centered = mean_pace_min_per_km - mean(mean_pace_min_per_km),
    dist_centered = distance_km - mean(distance_km),
    days_centered = days_since_start - mean(days_since_start),
    heat_centered = cumulative_heat_exposure_cd - mean(cumulative_heat_exposure_cd)
  ) %>%
  arrange(start_date_utc)

cat("=== Analytic dataset ===\n")
cat("N runs:", nrow(d), "\n")
cat("Span:", as.character(min(d$local_date)), "to", as.character(max(d$local_date)), "\n\n")

# --- Model 1: Fixed effects only, OLS via gls() so we can later add corAR1 ---
m_ols <- gls(
  mean_hr ~ pace_centered + dist_centered + days_centered + heat_centered,
  data = d,
  method = "REML"
)

cat("=== Model 1: OLS (no autocorrelation) ===\n")
print(summary(m_ols))
cat("\n")

# --- Model 2: Same fixed effects, AR(1) on residuals ---
# Runs are temporally ordered; consecutive-run residual correlation
# would inflate standard errors if ignored.
m_ar1 <- gls(
  mean_hr ~ pace_centered + dist_centered + days_centered + heat_centered,
  data = d,
  correlation = corAR1(form = ~ 1),
  method = "REML"
)

cat("=== Model 2: OLS + AR(1) residuals ===\n")
print(summary(m_ar1))
cat("\n")

cat("=== Model comparison (lower AIC = better) ===\n")
print(anova(m_ols, m_ar1))
cat("\n")

# --- Confidence intervals on the heat coefficient ---
cat("=== 95% CI on each fixed effect (Model 2) ===\n")
ci <- intervals(m_ar1, which = "coef")
print(ci)

# --- Effect size summary in interpretable units ---
cat("\n=== Interpretation ===\n")
coefs <- coef(m_ar1)
cat(sprintf("Per 1 min/km slower pace: %+.2f bpm\n", coefs["pace_centered"]))
cat(sprintf("Per 1 km longer:          %+.2f bpm\n", coefs["dist_centered"]))
cat(sprintf("Per 1 day later in block: %+.2f bpm\n", coefs["days_centered"]))
cat(sprintf("Per 1 C-day cumulative heat exposure: %+.3f bpm\n", coefs["heat_centered"]))
cat(sprintf("\nOver the full block (~%.0f C-days):\n",
            max(d$cumulative_heat_exposure_cd, na.rm = TRUE)))
total_heat_effect <- coefs["heat_centered"] * max(d$cumulative_heat_exposure_cd, na.rm = TRUE)
total_train_effect <- coefs["days_centered"] * max(d$days_since_start)
cat(sprintf("  Cumulative heat effect:     %+.1f bpm\n", total_heat_effect))
cat(sprintf("  Cumulative training effect: %+.1f bpm\n", total_train_effect))

# --- Residual diagnostics ---
d$resid_ar1 <- residuals(m_ar1, type = "normalized")
d$fitted_ar1 <- fitted(m_ar1)

# Save residual plot
library(ggplot2)
resid_plot <- ggplot(d, aes(x = fitted_ar1, y = resid_ar1)) +
  geom_point() + geom_hline(yintercept = 0, linetype = "dashed") +
  geom_smooth(method = "loess", se = FALSE) +
  labs(title = "Residuals vs Fitted (Model 2: OLS + AR1)",
       x = "Fitted HR (bpm)", y = "Normalized residual")
ggsave(here("output", "figures", "06_residuals_vs_fitted.png"), resid_plot,
       width = 7, height = 5, dpi = 120)

resid_time <- ggplot(d, aes(x = local_date, y = resid_ar1)) +
  geom_point() + geom_hline(yintercept = 0, linetype = "dashed") +
  geom_smooth(method = "loess", se = FALSE) +
  labs(title = "Residuals over time (look for unmodeled trend)",
       x = "Date", y = "Normalized residual")
ggsave(here("output", "figures", "07_residuals_over_time.png"), resid_time,
       width = 9, height = 5, dpi = 120)

# Save model objects for downstream use
saveRDS(m_ols, here("data", "processed", "m_ols.rds"))
saveRDS(m_ar1, here("data", "processed", "m_ar1.rds"))
write_parquet(d, here("data", "processed", "run_features_with_residuals.parquet"))

cat("\nDone. Saved 2 diagnostic plots, 2 model objects, residualized dataset.\n")
