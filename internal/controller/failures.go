package controller

import (
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kuberikcomv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

// maxFailures caps HealthCheck.status.failures (the CRD's MaxItems).
const maxFailures = 16

// maxFailureSourceLen is the CRD's MaxLength for a failure source.
const maxFailureSourceLen = 256

// severityFromLabel maps a Prometheus alert's "severity" label. critical and
// warning (case-insensitive) map to themselves; a missing or any other value is
// Critical, so an unmarked alert is never silently demoted.
func severityFromLabel(v string) kuberikcomv1alpha1.FailureSeverity {
	if strings.EqualFold(strings.TrimSpace(v), "warning") {
		return kuberikcomv1alpha1.FailureSeverityWarning
	}
	return kuberikcomv1alpha1.FailureSeverityCritical
}

// severityOverride reads the kuberik.com/severity annotation on the HealthCheck.
// It returns false when the annotation is absent or not Critical/Warning
// (unknown values are ignored).
func severityOverride(annotations map[string]string) (kuberikcomv1alpha1.FailureSeverity, bool) {
	switch v := kuberikcomv1alpha1.FailureSeverity(annotations[kuberikcomv1alpha1.SeverityAnnotation]); v {
	case kuberikcomv1alpha1.FailureSeverityCritical, kuberikcomv1alpha1.FailureSeverityWarning:
		return v, true
	}
	return "", false
}

// alertFailures builds one failure per FIRING alert (pending alerts are not
// failures). The source is "Alert <alertname>"; when several firing series share
// an alertname the labels that tell them apart (everything but alertname, the
// selector labels and severity) are appended in braces so each keeps its own
// entry. The HealthCheck's kuberik.com/severity annotation, when valid,
// overrides every alert's own severity.
func alertFailures(series []alertSeries, selector, annotations map[string]string, now metav1.Time) []kuberikcomv1alpha1.HealthCheckFailure {
	override, hasOverride := severityOverride(annotations)

	distinguishing := func(s alertSeries) map[string]string {
		out := map[string]string{}
		for k, v := range s.labels {
			if _, inSelector := selector[k]; inSelector || k == "alertname" || k == "severity" {
				continue
			}
			out[k] = v
		}
		return out
	}
	nameOf := func(s alertSeries) string {
		if n := s.labels["alertname"]; n != "" {
			return n
		}
		return "unnamed"
	}
	counts := map[string]int{}
	for _, s := range series {
		if s.firing {
			counts[nameOf(s)]++
		}
	}

	var out []kuberikcomv1alpha1.HealthCheckFailure
	for _, s := range series {
		if !s.firing {
			continue
		}
		name := nameOf(s)
		source := "Alert " + name
		extra := distinguishing(s)
		msg := "Alert firing"
		if len(extra) > 0 {
			msg = "Alert firing for " + formatAlertLabels(extra)
			if counts[name] > 1 {
				source += " {" + formatAlertLabels(extra) + "}"
			}
		}
		sev := severityFromLabel(s.labels["severity"])
		if hasOverride {
			sev = override
		}
		out = append(out, kuberikcomv1alpha1.HealthCheckFailure{
			Source:        truncate(source, maxFailureSourceLen),
			Kind:          kuberikcomv1alpha1.FailureKindAlert,
			Severity:      sev,
			Reason:        "AlertFiring",
			Message:       truncate(msg, 256),
			FirstSeenTime: now,
		})
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// mergeFailures folds the failures seen now into the recorded ones (the
// witness). Entries are keyed by source and kind and keep their first-seen time;
// entries no longer firing stay, so the list shrinks only when rollout-controller
// resets it at the next deploy or retry. At most maxFailures are kept, oldest
// first. Mirrors rollout-controller's mergeFailures.
func mergeFailures(recorded, current []kuberikcomv1alpha1.HealthCheckFailure) []kuberikcomv1alpha1.HealthCheckFailure {
	out := make([]kuberikcomv1alpha1.HealthCheckFailure, len(recorded))
	copy(out, recorded)
	for _, f := range current {
		idx := -1
		for i := range out {
			if out[i].Source == f.Source && out[i].Kind == f.Kind {
				idx = i
				break
			}
		}
		if idx >= 0 {
			f.FirstSeenTime = out[idx].FirstSeenTime
			out[idx] = f
			continue
		}
		out = append(out, f)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].FirstSeenTime.Before(&out[j].FirstSeenTime) })
	if len(out) > maxFailures {
		out = out[:maxFailures]
	}
	return out
}
