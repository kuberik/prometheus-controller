package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kuberikcomv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

func TestSeverityFromLabel(t *testing.T) {
	cases := map[string]kuberikcomv1alpha1.FailureSeverity{
		"critical": "Critical", "Critical": "Critical", "warning": "Warning", "WARNING": "Warning",
		"": "Critical", "page": "Critical", "info": "Critical",
	}
	for in, want := range cases {
		if got := severityFromLabel(in); got != want {
			t.Errorf("%q: got %s want %s", in, got, want)
		}
	}
}

func TestAlertFailuresNamingSeverityAndOverride(t *testing.T) {
	now := metav1.Now()
	series := []alertSeries{
		{labels: map[string]string{"alertname": "HighErrorRate", "severity": "warning", "app": "x"}, firing: true},
		{labels: map[string]string{"alertname": "Down", "pod": "a"}, firing: true},
		{labels: map[string]string{"alertname": "Down", "pod": "b", "severity": "warning"}, firing: true},
		{labels: map[string]string{"alertname": "Pending1"}, firing: false},
	}
	sel := map[string]string{"app": "x"}
	got := alertFailures(series, sel, nil, now)
	if len(got) != 3 {
		t.Fatalf("want 3 failures (pending excluded), got %d: %+v", len(got), got)
	}
	want := []struct {
		src string
		sev kuberikcomv1alpha1.FailureSeverity
	}{
		{"Alert HighErrorRate", "Warning"},
		{"Alert Down {pod=a}", "Critical"},
		{"Alert Down {pod=b}", "Warning"},
	}
	for i, w := range want {
		if got[i].Source != w.src || got[i].Severity != w.sev || got[i].Kind != kuberikcomv1alpha1.FailureKindAlert {
			t.Errorf("%d: got %+v want %+v", i, got[i], w)
		}
	}

	ann := map[string]string{kuberikcomv1alpha1.SeverityAnnotation: "Warning"}
	for _, f := range alertFailures(series, sel, ann, now) {
		if f.Severity != "Warning" {
			t.Errorf("override Warning not applied: %+v", f)
		}
	}
	ann[kuberikcomv1alpha1.SeverityAnnotation] = "Critical"
	for _, f := range alertFailures(series, sel, ann, now) {
		if f.Severity != "Critical" {
			t.Errorf("override Critical not applied: %+v", f)
		}
	}
	ann[kuberikcomv1alpha1.SeverityAnnotation] = "bogus"
	for _, f := range alertFailures(series, sel, ann, now) {
		if f.Severity != "Critical" {
			t.Errorf("an unknown annotation value counts as Critical, got %+v", f)
		}
	}
}

func TestMergeFailuresWitness(t *testing.T) {
	t0 := metav1.NewTime(time.Now().Add(-time.Hour))
	t1 := metav1.Now()
	rec := []kuberikcomv1alpha1.HealthCheckFailure{{Source: "Alert A", Kind: "Alert", Severity: "Warning", FirstSeenTime: t0}}
	// A recovered: not in current, stays. A again keeps firstSeen. B added.
	out := mergeFailures(rec, []kuberikcomv1alpha1.HealthCheckFailure{
		{Source: "Alert A", Kind: "Alert", Severity: "Critical", FirstSeenTime: t1},
		{Source: "Alert B", Kind: "Alert", Severity: "Critical", FirstSeenTime: t1},
	})
	if len(out) != 2 || !out[0].FirstSeenTime.Equal(&t0) || out[0].Severity != "Critical" || out[1].Source != "Alert B" {
		t.Fatalf("unexpected merge: %+v", out)
	}
	if out = mergeFailures(out, nil); len(out) != 2 {
		t.Fatalf("recovery must keep witness, got %+v", out)
	}
	var many []kuberikcomv1alpha1.HealthCheckFailure
	for i := 0; i < 20; i++ {
		many = append(many, kuberikcomv1alpha1.HealthCheckFailure{Source: "Alert " + string(rune('a'+i)), Kind: "Alert", FirstSeenTime: t1})
	}
	if got := mergeFailures(nil, many); len(got) != maxFailures {
		t.Fatalf("cap: got %d", len(got))
	}
}
