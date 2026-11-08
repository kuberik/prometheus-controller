package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	kuberikcomv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

var testNow = metav1.Now()

func TestFormatDecimal(t *testing.T) {
	cases := map[float64]string{0.05: "0.05", 1e-7: "0.0000001", 1e21: "1000000000000000000000", -0.0 * 1: "0", 12: "12", -3.5: "-3.5"}
	for in, want := range cases {
		got, ok := formatDecimal(in)
		if !ok || got != want || !decimalPattern.MatchString(got) {
			t.Errorf("formatDecimal(%v) = %q,%v want %q", in, got, ok, want)
		}
	}
	nan := 0.0
	nan = nan / nan
	if _, ok := formatDecimal(nan); ok {
		t.Error("NaN must not format")
	}
}

func TestParseObservationConfig(t *testing.T) {
	cfg, err := parseObservationConfig(map[string]string{
		AnnotationObservationLimit: "0.05", AnnotationObservationDirection: "above",
		AnnotationObservationUnit: "Ratio", AnnotationObservationName: "error rate",
	})
	if err != nil || !cfg.HasLimit || cfg.Limit != 0.05 || cfg.Direction != kuberikcomv1alpha1.ObservationDirectionAbove || cfg.Unit != "Ratio" || cfg.Name != "error rate" {
		t.Fatalf("unexpected: %+v %v", cfg, err)
	}
	for name, a := range map[string]map[string]string{
		"no direction":  {AnnotationObservationLimit: "5"},
		"bad direction": {AnnotationObservationLimit: "5", AnnotationObservationDirection: "up"},
		"exponent":      {AnnotationObservationLimit: "1e3", AnnotationObservationDirection: "Above"},
		"text":          {AnnotationObservationLimit: "five", AnnotationObservationDirection: "Above"},
	} {
		cfg, err := parseObservationConfig(a)
		if err == nil || cfg.HasLimit {
			t.Errorf("%s: expected error and no limit, got %+v %v", name, cfg, err)
		}
	}
	if cfg, err := parseObservationConfig(nil); err != nil || cfg.HasLimit {
		t.Errorf("empty annotations: %+v %v", cfg, err)
	}
}

func TestParseRuleExpr(t *testing.T) {
	ok := map[string]struct {
		limit float64
		dir   kuberikcomv1alpha1.HealthCheckObservationDirection
	}{
		"rate(errors_total[5m]) > 0.05": {0.05, kuberikcomv1alpha1.ObservationDirectionAbove},
		"up == 0 ":                      {},
		"avg(latency_seconds) >= 2":     {2, kuberikcomv1alpha1.ObservationDirectionAbove},
		"sum(x) / sum(y) < 0.9":         {0.9, kuberikcomv1alpha1.ObservationDirectionBelow},
		"node_load1 > 1e2":              {100, kuberikcomv1alpha1.ObservationDirectionAbove},
		"(a - b)\n  <= -3":              {-3, kuberikcomv1alpha1.ObservationDirectionBelow},
	}
	for expr, want := range ok {
		limit, dir, got := parseRuleExpr(expr)
		if want.dir == "" {
			if got {
				t.Errorf("%q should not parse", expr)
			}
			continue
		}
		if !got || limit != want.limit || dir != want.dir {
			t.Errorf("%q = %v %v %v", expr, limit, dir, got)
		}
	}
	for _, expr := range []string{"a > 1 and b > 2", "a > b", "a > 1 or b < 3", "a != 3", "a > bool 3", "vector(1)", "a > 1 unless b < 2"} {
		if _, _, got := parseRuleExpr(expr); got {
			t.Errorf("%q must not parse", expr)
		}
	}
}

func TestWorstValue(t *testing.T) {
	vals := []float64{0.2, 0.9, 0.1}
	if v, _ := worstValue(vals, kuberikcomv1alpha1.ObservationDirectionAbove); v != 0.9 {
		t.Errorf("above: %v", v)
	}
	if v, _ := worstValue(vals, kuberikcomv1alpha1.ObservationDirectionBelow); v != 0.1 {
		t.Errorf("below: %v", v)
	}
	if _, ok := worstValue(nil, kuberikcomv1alpha1.ObservationDirectionAbove); ok {
		t.Error("empty has no value")
	}
}

func rule(name, expr string, labels map[string]string, alerts ...promAlert) promRule {
	return promRule{Name: name, Query: expr, Labels: labels, Type: "alerting", Alerts: alerts}
}

func TestAlertObservations(t *testing.T) {
	healthy, unhealthy := kuberikcomv1alpha1.HealthStatusHealthy, kuberikcomv1alpha1.HealthStatusUnhealthy
	labels := map[string]string{"alertname": "HighErrorRate", "severity": "critical"}
	declared := map[string]string{AnnotationObservationLimit: "0.05", AnnotationObservationDirection: "Above", AnnotationObservationUnit: "Ratio"}
	firingAlert := func(v string) promAlert {
		return promAlert{State: "firing", Value: v, Labels: map[string]string{"alertname": "HighErrorRate", "severity": "critical"}}
	}
	r := rule("HighErrorRate", "rate(e[5m]) > 0.05", map[string]string{"severity": "critical"}, firingAlert("1.2e-01"), firingAlert("7e-02"))
	snap := matchAlerts([]promRule{r}, labels)

	t.Run("declared limit with firing alerts is a Threshold with the worst value", func(t *testing.T) {
		obs, err := alertObservations(declared, labels, &snap, nil, unhealthy, testNow)
		o := obs[0]
		if err != nil || len(obs) != 1 || o.Kind != kuberikcomv1alpha1.ObservationKindThreshold || o.Name != "HighErrorRate" ||
			o.Value != "0.12" || o.Limit != "0.05" || o.Direction != kuberikcomv1alpha1.ObservationDirectionAbove || o.Unit != "Ratio" || *o.Healthy {
			t.Fatalf("%+v %v", o, err)
		}
	})
	t.Run("declared limit, no active alert is a State", func(t *testing.T) {
		quiet := matchAlerts([]promRule{rule("HighErrorRate", "x > 1", map[string]string{"severity": "critical"})}, labels)
		obs, _ := alertObservations(declared, labels, &quiet, nil, healthy, testNow)
		if obs[0].Kind != kuberikcomv1alpha1.ObservationKindState || !*obs[0].Healthy || obs[0].Value != "" {
			t.Fatalf("%+v", obs[0])
		}
	})
	t.Run("declared limit, rules API unavailable is a State", func(t *testing.T) {
		obs, _ := alertObservations(declared, labels, nil, nil, healthy, testNow)
		if obs[0].Kind != kuberikcomv1alpha1.ObservationKindState {
			t.Fatalf("%+v", obs[0])
		}
	})
	t.Run("custom name and pending alert stay healthy", func(t *testing.T) {
		a := map[string]string{AnnotationObservationLimit: "0.05", AnnotationObservationDirection: "Above", AnnotationObservationName: "error rate"}
		pending := matchAlerts([]promRule{rule("HighErrorRate", "x > 1", map[string]string{"severity": "critical"},
			promAlert{State: "pending", Value: "9e-02", Labels: map[string]string{"alertname": "HighErrorRate"}})}, labels)
		obs, _ := alertObservations(a, labels, &pending, nil, kuberikcomv1alpha1.HealthStatusPending, testNow)
		if obs[0].Name != "error rate" || obs[0].Value != "0.09" || !*obs[0].Healthy {
			t.Fatalf("%+v", obs[0])
		}
	})
	t.Run("below direction takes the minimum", func(t *testing.T) {
		a := map[string]string{AnnotationObservationLimit: "100", AnnotationObservationDirection: "Below"}
		s := matchAlerts([]promRule{rule("HighErrorRate", "x", nil,
			promAlert{State: "firing", Value: "80"}, promAlert{State: "firing", Value: "20"})}, map[string]string{"alertname": "HighErrorRate"})
		obs, _ := alertObservations(a, labels, &s, nil, unhealthy, testNow)
		if obs[0].Value != "20" || obs[0].Limit != "100" {
			t.Fatalf("%+v", obs[0])
		}
	})
	t.Run("no annotations and a non-trivial rule is a Count of firing alerts", func(t *testing.T) {
		complexSnap := matchAlerts([]promRule{rule("HighErrorRate", "a > 1 and b > 2", map[string]string{"severity": "critical"}, firingAlert("1"), promAlert{State: "pending", Value: "1", Labels: map[string]string{"alertname": "HighErrorRate"}})}, labels)
		obs, _ := alertObservations(nil, labels, &complexSnap, nil, unhealthy, testNow)
		o := obs[0]
		if o.Kind != kuberikcomv1alpha1.ObservationKindCount || o.Name != "firing alerts" || o.Value != "1" || *o.Healthy {
			t.Fatalf("%+v", o)
		}
	})
	t.Run("no annotations and no alerts is a healthy Count of 0", func(t *testing.T) {
		obs, _ := alertObservations(nil, labels, nil, nil, healthy, testNow)
		if obs[0].Kind != kuberikcomv1alpha1.ObservationKindCount || obs[0].Value != "0" || !*obs[0].Healthy {
			t.Fatalf("%+v", obs[0])
		}
	})
	t.Run("pending-only is a healthy Count of 0", func(t *testing.T) {
		p := matchAlerts([]promRule{rule("HighErrorRate", "vector(1)", map[string]string{"severity": "critical"}, promAlert{State: "pending", Value: "1", Labels: map[string]string{"alertname": "HighErrorRate"}})}, labels)
		obs, _ := alertObservations(nil, labels, &p, nil, kuberikcomv1alpha1.HealthStatusPending, testNow)
		if obs[0].Value != "0" || !*obs[0].Healthy {
			t.Fatalf("%+v", obs[0])
		}
	})
	t.Run("simple rule expression infers a Threshold without annotations", func(t *testing.T) {
		obs, _ := alertObservations(nil, labels, &snap, nil, unhealthy, testNow)
		o := obs[0]
		if o.Kind != kuberikcomv1alpha1.ObservationKindThreshold || o.Limit != "0.05" || o.Value != "0.12" {
			t.Fatalf("%+v", o)
		}
	})
	t.Run("inferred limit with no active alert falls back to Count", func(t *testing.T) {
		quiet := matchAlerts([]promRule{rule("HighErrorRate", "x > 1", map[string]string{"severity": "critical"})}, labels)
		obs, _ := alertObservations(nil, labels, &quiet, nil, healthy, testNow)
		if obs[0].Kind != kuberikcomv1alpha1.ObservationKindCount {
			t.Fatalf("%+v", obs[0])
		}
	})
	t.Run("rules disagreeing on the limit are not inferred", func(t *testing.T) {
		two := matchAlerts([]promRule{rule("HighErrorRate", "x > 1", map[string]string{"severity": "critical"}, firingAlert("5")), rule("HighErrorRate", "x > 2", map[string]string{"severity": "critical"})}, labels)
		obs, _ := alertObservations(nil, labels, &two, nil, unhealthy, testNow)
		if obs[0].Kind != kuberikcomv1alpha1.ObservationKindCount {
			t.Fatalf("%+v", obs[0])
		}
	})
	t.Run("invalid annotations degrade to Count and report the error", func(t *testing.T) {
		complexSnap := matchAlerts([]promRule{rule("HighErrorRate", "a == 1", map[string]string{"severity": "critical"})}, labels)
		obs, err := alertObservations(map[string]string{AnnotationObservationLimit: "5"}, labels, &complexSnap, nil, healthy, testNow)
		if err == nil || obs[0].Kind != kuberikcomv1alpha1.ObservationKindCount {
			t.Fatalf("%+v %v", obs[0], err)
		}
	})
	t.Run("without the rules API the Count comes from the ALERTS series", func(t *testing.T) {
		res := &prometheusQueryResponse{}
		res.Data.Result = append(res.Data.Result, struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
			Values [][]interface{}   `json:"values"`
		}{Metric: map[string]string{"alertstate": "firing"}, Values: [][]interface{}{{1.0, "1"}}})
		obs, _ := alertObservations(nil, labels, nil, res, unhealthy, testNow)
		if obs[0].Value != "1" || *obs[0].Healthy {
			t.Fatalf("%+v", obs[0])
		}
	})
}

func TestMatchAlerts(t *testing.T) {
	rules := []promRule{
		rule("A", "x > 1", map[string]string{"severity": "critical"}, promAlert{State: "firing", Value: "2", Labels: map[string]string{"alertname": "A", "instance": "i1"}}),
		rule("A", "x > 1", map[string]string{"severity": "warning"}),
		rule("B", "x > 1", nil),
	}
	if s := matchAlerts(rules, map[string]string{"alertname": "A", "severity": "critical"}); len(s.Rules) != 1 || len(s.Alerts) != 1 {
		t.Errorf("rule labels: %+v", s)
	}
	if s := matchAlerts(rules, map[string]string{"alertname": "A", "instance": "i1"}); len(s.Rules) != 2 && len(s.Alerts) != 1 {
		t.Errorf("series label: %+v", s)
	} else if len(s.Rules) != 1 {
		t.Errorf("series label rule count: %d", len(s.Rules))
	}
	if s := matchAlerts(rules, map[string]string{"alertname": "A", "instance": "zz"}); len(s.Rules) != 0 {
		t.Errorf("unmatched series label: %+v", s)
	}
	if s := matchAlerts(rules, map[string]string{"severity": "critical"}); len(s.Rules) != 1 {
		t.Errorf("no alertname: %+v", s)
	}
}

func TestMergeObservationsKeepsObservedTime(t *testing.T) {
	old := metav1.NewTime(testNow.Add(-1e9 * 60))
	newer := testNow
	existing := []observation{{Name: "a", Kind: kuberikcomv1alpha1.ObservationKindCount, Value: "0", Healthy: boolPtr(true), ObservedTime: &old}}
	merged, changed := mergeObservations(existing, []observation{{Name: "a", Kind: kuberikcomv1alpha1.ObservationKindCount, Value: "0", Healthy: boolPtr(true), ObservedTime: &newer}})
	if changed || !merged[0].ObservedTime.Equal(&old) {
		t.Errorf("unchanged observation must keep its time: %+v %v", merged[0], changed)
	}
	merged, changed = mergeObservations(existing, []observation{{Name: "a", Kind: kuberikcomv1alpha1.ObservationKindCount, Value: "1", Healthy: boolPtr(false), ObservedTime: &newer}})
	if !changed || !merged[0].ObservedTime.Equal(&newer) {
		t.Errorf("changed observation must take the new time: %+v %v", merged[0], changed)
	}
}

func TestIgnoreObservationOnlyUpdates(t *testing.T) {
	p := ignoreObservationOnlyUpdates()
	a := &kuberikcomv1alpha1.HealthCheck{}
	b := a.DeepCopy()
	b.Status.Observations = []observation{{Name: "x", Kind: kuberikcomv1alpha1.ObservationKindCount, Value: "1"}}
	if p.Update(event.UpdateEvent{ObjectOld: a, ObjectNew: b}) {
		t.Error("observation-only change must be dropped")
	}
	c := b.DeepCopy()
	c.Status.Message = new(string)
	if !p.Update(event.UpdateEvent{ObjectOld: b, ObjectNew: c}) {
		t.Error("message change must pass")
	}
	d := b.DeepCopy()
	d.Annotations = map[string]string{"k": "v"}
	if !p.Update(event.UpdateEvent{ObjectOld: b, ObjectNew: d}) {
		t.Error("annotation change must pass")
	}
}
