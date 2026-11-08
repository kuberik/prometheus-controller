package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kuberikcomv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

// promRulesResponse is the response of GET /api/v1/rules?type=alert.
type promRulesResponse struct {
	Status string `json:"status"`
	Data   struct {
		Groups []struct {
			Rules []promRule `json:"rules"`
		} `json:"groups"`
	} `json:"data"`
	Error     string `json:"error"`
	ErrorType string `json:"errorType"`
}

type promRule struct {
	Name   string            `json:"name"`
	Query  string            `json:"query"`
	Labels map[string]string `json:"labels"`
	Type   string            `json:"type"`
	Alerts []promAlert       `json:"alerts"`
}

type promAlert struct {
	Labels map[string]string `json:"labels"`
	State  string            `json:"state"`
	// Value is the evaluated value of the rule expression, e.g. "1.2e+01".
	Value string `json:"value"`
}

// RuleQuerier is implemented by AlertQuerier implementations that can also list
// alerting rules with their active alerts. It is optional: without it (or when the
// call fails) observations degrade to a Count of the alerts seen in ALERTS.
type RuleQuerier interface {
	QueryRules(ctx context.Context, prometheusURL string, auth *prometheusAuth) ([]promRule, error)
}

// QueryRules lists the alerting rules, with each rule's active alerts and their
// evaluated values, from the Prometheus rules API.
func (q *DefaultAlertQuerier) QueryRules(ctx context.Context, prometheusURL string, auth *prometheusAuth) ([]promRule, error) {
	httpClient := q.Client
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	u, err := url.Parse(strings.TrimRight(prometheusURL, "/") + "/api/v1/rules")
	if err != nil {
		return nil, fmt.Errorf("invalid Prometheus URL: %w", err)
	}
	u.RawQuery = url.Values{"type": []string{"alert"}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	if auth != nil {
		if auth.Token != "" {
			req.Header.Set("Authorization", "Bearer "+auth.Token)
		} else if auth.Username != "" {
			req.SetBasicAuth(auth.Username, auth.Password)
		}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query Prometheus rules: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Prometheus rules API returned status %d: %s", resp.StatusCode, string(body))
	}
	var parsed promRulesResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("failed to unmarshal rules response: %w", err)
	}
	if parsed.Status != "success" {
		return nil, fmt.Errorf("Prometheus rules query failed: %s (%s)", parsed.Error, parsed.ErrorType)
	}
	var rules []promRule
	for _, g := range parsed.Data.Groups {
		for _, r := range g.Rules {
			if r.Type == "" || r.Type == "alerting" {
				rules = append(rules, r)
			}
		}
	}
	return rules, nil
}

// alertSnapshot is the part of the rules API response that matches the HealthCheck's labels.
type alertSnapshot struct {
	Rules  []promRule
	Alerts []promAlert
}

func labelsContain(have, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

// matchAlerts selects the rules and active alerts selected by the HealthCheck's
// alert labels. A rule matches by name for the alertname label and by its own
// labels for the rest; a label the rule does not carry (it comes from the series,
// e.g. instance) must be matched by one of its active alerts.
func matchAlerts(rules []promRule, filter map[string]string) alertSnapshot {
	var snap alertSnapshot
	for _, r := range rules {
		if want, ok := filter["alertname"]; ok && r.Name != want {
			continue
		}
		conflict, needsAlert := false, false
		for k, v := range filter {
			if k == "alertname" {
				continue
			}
			have, ok := r.Labels[k]
			if !ok {
				needsAlert = true
			} else if have != v {
				conflict = true
			}
		}
		if conflict {
			continue
		}
		var matched []promAlert
		for _, a := range r.Alerts {
			labels := map[string]string{"alertname": r.Name}
			for k, v := range r.Labels {
				labels[k] = v
			}
			for k, v := range a.Labels {
				labels[k] = v
			}
			if labelsContain(labels, filter) {
				matched = append(matched, a)
			}
		}
		// Every filter label the rule does not carry needs an alert that has it.
		if needsAlert && len(matched) == 0 {
			continue
		}
		snap.Rules = append(snap.Rules, r)
		snap.Alerts = append(snap.Alerts, matched...)
	}
	return snap
}

// inferLimit returns the limit and direction shared by every matched rule, if
// each has a trivial `expr > N` / `expr < N` expression.
func (s alertSnapshot) inferLimit() (float64, kuberikcomv1alpha1.HealthCheckObservationDirection, bool) {
	var (
		limit float64
		dir   kuberikcomv1alpha1.HealthCheckObservationDirection
	)
	for i, r := range s.Rules {
		l, d, ok := parseRuleExpr(r.Query)
		if !ok || (i > 0 && (l != limit || d != dir)) {
			return 0, "", false
		}
		limit, dir = l, d
	}
	return limit, dir, len(s.Rules) > 0
}

// active returns the values of pending and firing alerts and whether any is firing.
func (s alertSnapshot) active() (values []float64, firing bool) {
	for _, a := range s.Alerts {
		if a.State != "firing" && a.State != "pending" {
			continue
		}
		if a.State == "firing" {
			firing = true
		}
		if v, err := strconv.ParseFloat(strings.TrimSpace(a.Value), 64); err == nil {
			values = append(values, v)
		}
	}
	return values, firing
}

func (s alertSnapshot) firingCount() int {
	n := 0
	for _, a := range s.Alerts {
		if a.State == "firing" {
			n++
		}
	}
	return n
}

// firingSeriesInResult counts the distinct firing series seen in an ALERTS query
// result, used for the Count when the rules API is unavailable.
func firingSeriesInResult(result *prometheusQueryResponse) int {
	if result == nil {
		return 0
	}
	n := 0
	for _, series := range result.Data.Result {
		if series.Metric["alertstate"] != "firing" {
			continue
		}
		if len(series.Value) >= 2 {
			if s, ok := series.Value[1].(string); ok && s == "1" {
				n++
				continue
			}
		}
		for _, v := range series.Values {
			if len(v) >= 2 {
				if s, ok := v[1].(string); ok && s == "1" {
					n++
					break
				}
			}
		}
	}
	return n
}

// alertObservations builds the observation for the HealthCheck.
//
//   - A limit (from the observation-limit/-direction annotations, or inferred from
//     a trivial rule expression when the annotations are absent) plus a known
//     evaluated value of an active alert gives a Threshold.
//   - A limit declared by annotation but no active alert (so no value) gives a
//     State, healthy unless the ALERTS lookup says an alert fired.
//   - Anything else gives a Count of firing alerts.
//
// snap is nil when the rules API was unavailable.
func alertObservations(annotations, alertLabels map[string]string, snap *alertSnapshot, result *prometheusQueryResponse, status kuberikcomv1alpha1.HealthStatus, now metav1.Time) (obs []observation, cfgErr error) {
	cfg, cfgErr := parseObservationConfig(annotations)
	declared := cfg.HasLimit
	if !declared && snap != nil {
		if limit, dir, ok := snap.inferLimit(); ok {
			cfg.Limit, cfg.Direction, cfg.HasLimit = limit, dir, true
		}
	}

	name := cfg.Name
	if name == "" {
		name = alertLabels["alertname"]
	}
	if name == "" && snap != nil && len(snap.Rules) > 0 {
		name = snap.Rules[0].Name
	}
	if name == "" {
		name = "alert"
	}
	cfg.Name = name

	if cfg.HasLimit {
		if snap != nil {
			values, firing := snap.active()
			if v, ok := worstValue(values, cfg.Direction); ok {
				if o, ok := thresholdObservation(cfg, v, firing, now); ok {
					return []observation{o}, cfgErr
				}
			}
		}
		if declared {
			return []observation{stateObservation(name, status != kuberikcomv1alpha1.HealthStatusUnhealthy, now)}, cfgErr
		}
	}

	firing := firingSeriesInResult(result)
	if snap != nil {
		firing = snap.firingCount()
	}
	return []observation{countObservation(firing, now)}, cfgErr
}
