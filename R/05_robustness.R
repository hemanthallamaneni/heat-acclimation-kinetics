# Leave-one-out robustness for the heat coefficient.
# At n=30 with p=0.047, we need to confirm no single observation is driving
# the result. Refit the v2 model 30 times, each time omitting one run,
# and report the distribution of the heat coefficient and its p-value.

suppressPackageStartupMessages({
  library(tidyverse); library(arrow); library(here); library(nlme)
})

d <- read_parquet(here("data", "processed", "run_features_v2.parquet"))

results <- map_dfr(1:nrow(d), function(i) {
  d_minus <- d[-i, ]
  fit <- tryCatch(
    gls(mean_hr ~ pace_centered + dist_centered + days_centered + heat_v2_centered,
        data = d_minus, correlation = corAR1(form = ~ 1), method = "REML"),
    error = function(e) NULL
  )
  if (is.null(fit)) return(tibble(omitted_idx = i, heat_coef = NA, heat_p = NA))
  s <- summary(fit)$tTable
  tibble(
    omitted_idx       = i,
    omitted_date      = d$local_date[i],
    omitted_activity  = d$activity_id[i],
    heat_coef         = s["heat_v2_centered", "Value"],
    heat_p            = s["heat_v2_centered", "p-value"]
  )
})

cat("=== Leave-one-out heat coefficient ===\n")
cat(sprintf("Original:         coef = -0.217,  p = 0.047\n"))
cat(sprintf("LOO median coef:  %.3f\n", median(results$heat_coef, na.rm = TRUE)))
cat(sprintf("LOO range coef:   %.3f to %.3f\n",
            min(results$heat_coef, na.rm = TRUE), max(results$heat_coef, na.rm = TRUE)))
cat(sprintf("LOO median p:     %.3f\n", median(results$heat_p, na.rm = TRUE)))
cat(sprintf("LOO p < 0.05 in   %d of %d iterations\n",
            sum(results$heat_p < 0.05, na.rm = TRUE), nrow(results)))
cat("\n=== Most-influential omissions (largest p-value when omitted) ===\n")
print(results %>% arrange(desc(heat_p)) %>% head(5))
cat("\n=== Least-influential omissions (smallest p-value when omitted) ===\n")
print(results %>% arrange(heat_p) %>% head(5))

# Save
write_parquet(results, here("data", "processed", "loo_results.parquet"))
