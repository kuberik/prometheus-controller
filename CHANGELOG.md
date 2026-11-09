# Changelog

## [Unreleased]

### Added

- Write `status.observations` on `prometheus-alert` HealthChecks: a Threshold (limit from `healthcheck.kuberik.com/observation-limit`/`-direction`/`-unit`/`-name`, or inferred from a trivial rule expression), a State when the limit is declared but no alert is active, otherwise a Count of firing alerts. Requires rollout-controller v0.11.0 (`go.mod`, `vendir.yml`); Count, State and Threshold are built by its shared `pkg/healthcheck/observation` package.
