package controller

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	kuberikcomv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

type observation = kuberikcomv1alpha1.HealthCheckObservation

const (
	// maxObservations mirrors the CRD's MaxItems on status.observations.
	maxObservations = 16

	firingAlertsObservationName = "firing alerts"

	// Annotations that declare the limit of the alert's condition, which the
	// Prometheus alerts API does not expose.
	AnnotationObservationLimit     = "healthcheck.kuberik.com/observation-limit"
	AnnotationObservationDirection = "healthcheck.kuberik.com/observation-direction"
	AnnotationObservationUnit      = "healthcheck.kuberik.com/observation-unit"
	AnnotationObservationName      = "healthcheck.kuberik.com/observation-name"
)

var decimalPattern = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`)

// formatDecimal renders v as a plain decimal matching the CRD pattern
// ^-?[0-9]+(\.[0-9]+)?$: no exponent, no suffix. ok is false for NaN/Inf.
func formatDecimal(v float64) (string, bool) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return "", false
	}
	s := strconv.FormatFloat(v, 'f', -1, 64)
	if s == "-0" {
		s = "0"
	}
	return s, true
}

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
	if !decimalPattern.MatchString(limitStr) {
		return cfg, invalidAnnotation(AnnotationObservationLimit, limitStr, "expected a decimal such as 0.05")
	}
	limit, err := strconv.ParseFloat(limitStr, 64)
	if err != nil {
		return cfg, invalidAnnotation(AnnotationObservationLimit, limitStr, err.Error())
	}
	dir, ok := parseDirection(annotations[AnnotationObservationDirection])
	if !ok {
		return cfg, invalidAnnotation(AnnotationObservationDirection, annotations[AnnotationObservationDirection], "expected Above or Below")
	}
	cfg.Limit, cfg.Direction, cfg.HasLimit = limit, dir, true
	return cfg, nil
}

func invalidAnnotation(key, value, why string) error {
	return fmt.Errorf("invalid annotation %s=%q: %s", key, value, why)
}

func parseDirection(s string) (kuberikcomv1alpha1.HealthCheckObservationDirection, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "above":
		return kuberikcomv1alpha1.ObservationDirectionAbove, true
	case "below":
		return kuberikcomv1alpha1.ObservationDirectionBelow, true
	}
	return "", false
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

func boolPtr(b bool) *bool { return &b }

// countObservation is the Count of firing alerts; healthy while there are none.
// Pending alerts are not counted and do not make it unhealthy.
func countObservation(firing int, now metav1.Time) observation {
	return observation{
		Name:         firingAlertsObservationName,
		Kind:         kuberikcomv1alpha1.ObservationKindCount,
		Value:        strconv.Itoa(firing),
		Healthy:      boolPtr(firing == 0),
		ObservedTime: &now,
	}
}

// thresholdObservation is a Threshold; healthy is false only while an alert is
// firing. ok is false if the value or limit cannot be rendered as a decimal.
func thresholdObservation(cfg observationConfig, value float64, firing bool, now metav1.Time) (observation, bool) {
	valueStr, ok1 := formatDecimal(value)
	limitStr, ok2 := formatDecimal(cfg.Limit)
	if !ok1 || !ok2 {
		return observation{}, false
	}
	return observation{
		Name:         cfg.Name,
		Kind:         kuberikcomv1alpha1.ObservationKindThreshold,
		Value:        valueStr,
		Limit:        limitStr,
		Direction:    cfg.Direction,
		Unit:         cfg.Unit,
		Healthy:      boolPtr(!firing),
		ObservedTime: &now,
	}, true
}

// stateObservation carries only the verdict, for a declared limit with no value known.
func stateObservation(name string, healthy bool, now metav1.Time) observation {
	return observation{
		Name:         name,
		Kind:         kuberikcomv1alpha1.ObservationKindState,
		Healthy:      boolPtr(healthy),
		ObservedTime: &now,
	}
}

// sortObservations puts failing observations first, then by name, and caps the list.
func sortObservations(obs []observation) []observation {
	rank := func(o observation) int {
		switch {
		case o.Healthy != nil && !*o.Healthy:
			return 0
		case o.Healthy == nil:
			return 1
		}
		return 2
	}
	sort.SliceStable(obs, func(i, j int) bool {
		if ri, rj := rank(obs[i]), rank(obs[j]); ri != rj {
			return ri < rj
		}
		return obs[i].Name < obs[j].Name
	})
	if len(obs) > maxObservations {
		obs = obs[:maxObservations]
	}
	return obs
}

// mergeObservations keeps the previous ObservedTime of observations that did not
// change, so a steady value does not rewrite status (and wake watchers) on every
// poll. It reports whether the list differs from existing.
func mergeObservations(existing, updated []observation) ([]observation, bool) {
	merged := make([]observation, len(updated))
	changed := len(existing) != len(updated)
	for i, o := range updated {
		merged[i] = o
		if i < len(existing) {
			prev, next := existing[i], o
			prev.ObservedTime, next.ObservedTime = nil, nil
			if equality.Semantic.DeepEqual(prev, next) && existing[i].ObservedTime != nil {
				merged[i].ObservedTime = existing[i].ObservedTime
				continue
			}
		}
		changed = true
	}
	return merged, changed
}

// ignoreObservationOnlyUpdates drops HealthCheck update events whose only change
// is status.observations, which this controller writes itself; a drifting alert
// value would otherwise retrigger a reconcile on every poll.
func ignoreObservationOnlyUpdates() predicate.Predicate {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		oldHC, ok1 := e.ObjectOld.(*kuberikcomv1alpha1.HealthCheck)
		newHC, ok2 := e.ObjectNew.(*kuberikcomv1alpha1.HealthCheck)
		if !ok1 || !ok2 || equality.Semantic.DeepEqual(oldHC.Status.Observations, newHC.Status.Observations) {
			return true
		}
		oldStatus, newStatus := oldHC.Status.DeepCopy(), newHC.Status.DeepCopy()
		oldStatus.Observations, newStatus.Observations = nil, nil
		return !(oldHC.Generation == newHC.Generation &&
			equality.Semantic.DeepEqual(oldHC.Labels, newHC.Labels) &&
			equality.Semantic.DeepEqual(oldHC.Annotations, newHC.Annotations) &&
			equality.Semantic.DeepEqual(oldStatus, newStatus))
	}}
}
