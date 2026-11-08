# Changelog

## [Unreleased]

### Added

- Write `status.observations` on `prometheus-alert` HealthChecks: a Threshold (limit from `healthcheck.kuberik.com/observation-limit`/`-direction`/`-unit`/`-name`, or inferred from a trivial rule expression), a State when the limit is declared but no alert is active, otherwise a Count of firing alerts. Requires the rollout-controller release that adds the field; `go.mod` and `vendir.yml` currently point at a local worktree and must be swapped for it.
