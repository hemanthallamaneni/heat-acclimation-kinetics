library(tidyverse)
library(here)

# Verify renv project is active
renv::status()

# Quick data manipulation test
data <- tibble(
  x = 1:10,
  y = rnorm(10, mean = x, sd = 0.5)
)

print(data)

# Quick model test (the actual library we'll use this week)
library(nlme)
fit <- gls(y ~ x, data = data)
summary(fit)

# Quick plot test
ggplot(data, aes(x = x, y = y)) +
  geom_point() +
  geom_smooth(method = "lm") +
  labs(title = "Environment verification — should render in VS Code plot pane")