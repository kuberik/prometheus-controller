package controller

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	kuberikcomv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
	obs "github.com/kuberik/rollout-controller/pkg/healthcheck/observation"
)

type observation = kuberikcomv1alpha1.HealthCheckObservation

const (
	firingAlertsObservationName = "firing alerts"

	// Annotations that declare the limit of the alert's condition, which the
	// Prometheus alerts API does not expose.
	AnnotationObservationLimit     = "healthcheck.kuberik.com/observation-limit"
	AnnotationObservationDirection = "healthcheck.kuberik.com/observation-direction"
	AnnotationObservationUnit      = "healthcheck.kuberik.com/observation-unit"
	AnnotationObservationName      = "healthcheck.kuberik.com/observation-name"
)

// observationConfig is the user-declared (or rule-inferred) limit of an alert.
type observationConfig struct {
	Name      string
	Limit     float64
	Direction kuberikcomv1alpha1.HealthCheckObservationDirection
	Unit      string
	// HasLimit is false when no usable limit is known; the observation is then a Count.
	HasLimit bool
}

// parseObservationConfig reads the observation-* annotations. A limit without a
// valid direction, or an unparsable limit, yields HasLimit=false and an error
// for the caller to log; the observation then degrades to a Count.
func parseObservationConfig(annotations map[string]string) (observationConfig, error) {
	cfg := observationConfig{
		Name: strings.TrimSpace(annotations[AnnotationObservationName]),
		Unit: strings.TrimSpace(annotations[AnnotationObservationUnit]),
	}
	limitStr := strings.TrimSpace(annotations[AnnotationObservationLimit])
	if limitStr == "" {
		return cfg, nil
	}
	if !obs.DecimalPattern.MatchString(limitStr) {
		return cfg, invalidAnnotation(AnnotationObservationLimit, limitStr, "expected a decimal such as 0.05")
	}
	limit, err := strconv.ParseFloat(limitStr, 64)
	if err != nil {
		return cfg, invalidAnnotation(AnnotationObservationLimit, limitStr, err.Error())
	}
	dir, ok := obs.ParseDirection(annotations[AnnotationObservationDirection])
	if !ok {
		return cfg, invalidAnnotation(AnnotationObservationDirection, annotations[AnnotationObservationDirection], "expected Above or Below")
	}
	cfg.Limit, cfg.Direction, cfg.HasLimit = limit, dir, true
	return cfg, nil
}

func invalidAnnotation(key, value, why string) error {
	return fmt.Errorf("invalid annotation %s=%q: %s", key, value, why)
}

// ruleExprPattern matches a rule expression of the form `<left> <cmp> <number>`.
var ruleExprPattern = regexp.MustCompile(`(?s)^\s*(.+?)\s*(>=|<=|>|<)\s*(-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)\s*$`)

// compoundExpr detects a left side with another comparison or a set operator,
// where the trailing comparison is not the whole alert condition.
var compoundExpr = regexp.MustCompile(`(?i)[<>=!]|\b(and|or|unless|bool)\b`)

// parseRuleExpr infers limit and direction from a trivial alert expression such
// as `rate(errors[5m]) > 0.05`. ok is false for anything more involved
// (compound conditions, `==`, `!=`, comparisons between two series, `bool`).
func parseRuleExpr(expr string) (limit float64, dir kuberikcomv1alpha1.HealthCheckObservationDirection, ok bool) {
	m := ruleExprPattern.FindStringSubmatch(expr)
	if m == nil || compoundExpr.MatchString(m[1]) {
		return 0, "", false
	}
	limit, err := strconv.ParseFloat(m[3], 64)
	if err != nil {
		return 0, "", false
	}
	if m[2] == ">" || m[2] == ">=" {
		return limit, kuberikcomv1alpha1.ObservationDirectionAbove, true
	}
	return limit, kuberikcomv1alpha1.ObservationDirectionBelow, true
}

// worstValue picks the value furthest past the limit: the maximum for Above,
// the minimum for Below.
func worstValue(values []float64, dir kuberikcomv1alpha1.HealthCheckObservationDirection) (float64, bool) {
	var best float64
	found := false
	for _, v := range values {
		if math.IsNaN(v) {
			continue
		}
		if !found ||
			(dir == kuberikcomv1alpha1.ObservationDirectionAbove && v > best) ||
			(dir == kuberikcomv1alpha1.ObservationDirectionBelow && v < best) {
			best, found = v, true
		}
	}
	return best, found
}
