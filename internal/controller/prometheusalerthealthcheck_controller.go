/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	kuberikcomv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
	obs "github.com/kuberik/rollout-controller/pkg/healthcheck/observation"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// HealthCheckClassPrometheusAlert is the class for Prometheus alert-based health checks
	HealthCheckClassPrometheusAlert = "prometheus-alert"

	// Annotation keys for Prometheus alert configuration
	AnnotationPrometheusURL               = "healthcheck.kuberik.com/prometheus-url"
	AnnotationPrometheusAlertLabels       = "healthcheck.kuberik.com/prometheus-alert-labels"
	AnnotationPrometheusCredentialsSecret = "healthcheck.kuberik.com/prometheus-credentials-secret"
	AnnotationRequeueInterval             = "healthcheck.kuberik.com/requeue-interval"
	AnnotationLastScrapeTime              = "healthcheck.kuberik.com/last-scrape-time"

	// Secret keys for Prometheus credentials
	SecretKeyToken    = "token"
	SecretKeyUsername = "username"
	SecretKeyPassword = "password"
)

// prometheusQueryResponse represents the response from the Prometheus query API
type prometheusQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`  // for instant query: [timestamp, value]
			Values [][]interface{}   `json:"values"` // for range query: [[timestamp, value], ...]
		} `json:"result"`
	} `json:"data"`
	Error     string `json:"error"`
	ErrorType string `json:"errorType"`
}

// AlertQuerier is an interface for querying Prometheus alerts
type AlertQuerier interface {
	QueryAlerts(ctx context.Context, prometheusURL, query string, start, end time.Time, auth *prometheusAuth) (*prometheusQueryResponse, error)
}

type prometheusAuth struct {
	Token    string
	Username string
	Password string
}

// DefaultAlertQuerier is the default implementation that calls the real Prometheus API
type DefaultAlertQuerier struct {
	Client *http.Client
}

// QueryAlerts queries the Prometheus range query API for ALERTS
func (q *DefaultAlertQuerier) QueryAlerts(ctx context.Context, prometheusURL, query string, start, end time.Time, auth *prometheusAuth) (*prometheusQueryResponse, error) {
	httpClient := q.Client
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	u, err := url.Parse(strings.TrimRight(prometheusURL, "/") + "/api/v1/query_range")
	if err != nil {
		return nil, fmt.Errorf("invalid Prometheus URL: %w", err)
	}

	params := url.Values{}
	params.Set("query", query)
	params.Set("start", fmt.Sprintf("%d", start.Unix()))
	params.Set("end", fmt.Sprintf("%d", end.Unix()))
	// Use step of 1 minute to get reasonable granularity
	step := end.Sub(start) / 60
	if step < time.Second {
		step = time.Second
	}
	params.Set("step", fmt.Sprintf("%d", int(step.Seconds())))
	u.RawQuery = params.Encode()

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
		return nil, fmt.Errorf("failed to query Prometheus: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("prometheus API returned status %d: %s", resp.StatusCode, string(body))
	}

	var result prometheusQueryResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if result.Status != "success" {
		return nil, fmt.Errorf("prometheus query failed: %s (%s)", result.Error, result.ErrorType)
	}

	return &result, nil
}

// PrometheusAlertHealthCheckReconciler reconciles HealthCheck objects with class "prometheus-alert"
type PrometheusAlertHealthCheckReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	AlertQuerier AlertQuerier
}

// +kubebuilder:rbac:groups=kuberik.com,resources=healthchecks,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=kuberik.com,resources=healthchecks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile fetches alert status from Prometheus and updates the HealthCheck status
func (r *PrometheusAlertHealthCheckReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Fetch the HealthCheck instance
	healthCheck := &kuberikcomv1alpha1.HealthCheck{}
	if err := r.Get(ctx, req.NamespacedName, healthCheck); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get HealthCheck")
		return ctrl.Result{}, err
	}

	// Check if this is a prometheus-alert health check
	if healthCheck.Spec.Class == nil || *healthCheck.Spec.Class != HealthCheckClassPrometheusAlert {
		return ctrl.Result{}, nil
	}

	// Get Prometheus URL from annotations
	prometheusURL, err := r.getPrometheusURL(healthCheck)
	if err != nil {
		log.Error(err, "Failed to get Prometheus URL")
		return r.updateHealthCheckStatus(ctx, healthCheck, kuberikcomv1alpha1.HealthStatusUnhealthy, err.Error())
	}

	// Get alert labels from annotations
	alertLabels, err := r.getAlertLabels(healthCheck)
	if err != nil {
		log.Error(err, "Failed to get alert labels")
		return r.updateHealthCheckStatus(ctx, healthCheck, kuberikcomv1alpha1.HealthStatusUnhealthy, err.Error())
	}

	// Get optional credentials
	auth, err := r.getCredentials(ctx, healthCheck)
	if err != nil {
		log.Error(err, "Failed to get Prometheus credentials")
		return r.updateHealthCheckStatus(ctx, healthCheck, kuberikcomv1alpha1.HealthStatusUnhealthy, fmt.Sprintf("Failed to get credentials: %v", err))
	}

	// Build the ALERTS query from labels
	query := buildAlertsQuery(alertLabels)

	// Determine time range: from last scrape to now
	now := time.Now()
	lastScrape := r.getLastScrapeTime(healthCheck)
	if lastScrape.IsZero() {
		// First scrape: look back by the requeue interval
		lastScrape = now.Add(-r.getRequeueInterval(healthCheck))
	}

	// Get alert querier
	querier := r.AlertQuerier
	if querier == nil {
		querier = &DefaultAlertQuerier{}
	}

	// Query Prometheus
	result, err := querier.QueryAlerts(ctx, prometheusURL, query, lastScrape, now, auth)
	if err != nil {
		log.Error(err, "Failed to query Prometheus", "query", query)
		errMsg := fmt.Sprintf("Failed to query Prometheus: %v", err)
		return r.updateHealthCheckStatus(ctx, healthCheck, kuberikcomv1alpha1.HealthStatusUnhealthy, errMsg)
	}

	// Update last scrape time
	if err := r.setLastScrapeTime(ctx, healthCheck, now); err != nil {
		log.Error(err, "Failed to update last scrape time")
		return ctrl.Result{}, err
	}

	// Check if any alert is firing in the range
	healthStatus, message := r.evaluateAlertResult(result, alertLabels)

	// Observations are best effort: a failing rules lookup or bad annotation
	// degrades them, never the status.
	var snap *alertSnapshot
	if rq, ok := querier.(RuleQuerier); ok {
		if rules, rerr := rq.QueryRules(ctx, prometheusURL, auth); rerr != nil {
			log.Error(rerr, "Failed to query Prometheus rules; observations degrade to ALERTS only")
		} else {
			matched := matchAlerts(rules, alertLabels)
			snap = &matched
		}
	}
	observations, cfgErr := alertObservations(healthCheck.Annotations, alertLabels, snap, result, healthStatus, metav1.NewTime(now))
	if cfgErr != nil {
		log.Error(cfgErr, "Ignoring observation limit annotations")
	}

	log.Info("Evaluated Prometheus alerts",
		"query", query,
		"healthStatus", healthStatus,
		"resultCount", len(result.Data.Result))

	return r.updateHealthCheckStatus(ctx, healthCheck, healthStatus, message, observations...)
}

// getPrometheusURL extracts the Prometheus URL from HealthCheck annotations
func (r *PrometheusAlertHealthCheckReconciler) getPrometheusURL(healthCheck *kuberikcomv1alpha1.HealthCheck) (string, error) {
	if healthCheck.Annotations == nil {
		return "", fmt.Errorf("no annotations found on HealthCheck")
	}

	prometheusURL, exists := healthCheck.Annotations[AnnotationPrometheusURL]
	if !exists || prometheusURL == "" {
		return "", fmt.Errorf("annotation %s not found or empty", AnnotationPrometheusURL)
	}

	return prometheusURL, nil
}

// getAlertLabels extracts alert labels from HealthCheck annotations
// Format: comma-separated key=value pairs, e.g. "alertname=MyAlert,severity=critical"
func (r *PrometheusAlertHealthCheckReconciler) getAlertLabels(healthCheck *kuberikcomv1alpha1.HealthCheck) (map[string]string, error) {
	if healthCheck.Annotations == nil {
		return nil, fmt.Errorf("no annotations found on HealthCheck")
	}

	labelsStr, exists := healthCheck.Annotations[AnnotationPrometheusAlertLabels]
	if !exists || labelsStr == "" {
		return nil, fmt.Errorf("annotation %s not found or empty", AnnotationPrometheusAlertLabels)
	}

	labels := make(map[string]string)
	for _, pair := range strings.Split(labelsStr, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid label format %q, expected key=value", pair)
		}
		labels[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}

	if len(labels) == 0 {
		return nil, fmt.Errorf("no valid labels found in %s", AnnotationPrometheusAlertLabels)
	}

	return labels, nil
}

// buildAlertsQuery constructs a PromQL ALERTS query from the given labels
func buildAlertsQuery(labels map[string]string) string {
	parts := make([]string, 0, len(labels))
	for _, k := range sortedLabelKeys(labels) {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, k, labels[k]))
	}
	return fmt.Sprintf("ALERTS{%s}", strings.Join(parts, ","))
}

func sortedLabelKeys(labels map[string]string) []string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// formatAlertLabels renders labels as a sorted, human-readable list such as
// "alertname=HighErrorRate, app=checkout-api" for use in status messages.
func formatAlertLabels(labels map[string]string) string {
	parts := make([]string, 0, len(labels))
	for _, k := range sortedLabelKeys(labels) {
		parts = append(parts, k+"="+labels[k])
	}
	return strings.Join(parts, ", ")
}

// getCredentials retrieves optional Prometheus credentials from a Kubernetes Secret
func (r *PrometheusAlertHealthCheckReconciler) getCredentials(ctx context.Context, healthCheck *kuberikcomv1alpha1.HealthCheck) (*prometheusAuth, error) {
	if healthCheck.Annotations == nil {
		return nil, nil
	}

	secretRef, exists := healthCheck.Annotations[AnnotationPrometheusCredentialsSecret]
	if !exists || secretRef == "" {
		return nil, nil
	}

	// Parse secret reference (format: "name" or "namespace/name")
	secretNamespace := healthCheck.Namespace
	secretName := secretRef
	if parts := strings.Split(secretRef, "/"); len(parts) == 2 {
		secretNamespace = parts[0]
		secretName = parts[1]
	}

	// Fetch the secret
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: secretNamespace, Name: secretName}, secret); err != nil {
		return nil, fmt.Errorf("failed to get secret %s/%s: %w", secretNamespace, secretName, err)
	}

	auth := &prometheusAuth{}

	// Check for bearer token
	if token, exists := secret.Data[SecretKeyToken]; exists {
		auth.Token = string(token)
		return auth, nil
	}

	// Check for basic auth
	if username, exists := secret.Data[SecretKeyUsername]; exists {
		auth.Username = string(username)
		if password, exists := secret.Data[SecretKeyPassword]; exists {
			auth.Password = string(password)
		}
		return auth, nil
	}

	return nil, fmt.Errorf("secret %s/%s must contain either %q or %q key", secretNamespace, secretName, SecretKeyToken, SecretKeyUsername)
}

// evaluateAlertResult checks if any alert is firing in the query result
func (r *PrometheusAlertHealthCheckReconciler) evaluateAlertResult(result *prometheusQueryResponse, labels map[string]string) (kuberikcomv1alpha1.HealthStatus, string) {
	if len(result.Data.Result) == 0 {
		return kuberikcomv1alpha1.HealthStatusHealthy, fmt.Sprintf("No alerts firing for %s", formatAlertLabels(labels))
	}

	// Check all time series - if any has a firing value (1), the alert is firing
	for _, series := range result.Data.Result {
		alertState := series.Metric["alertstate"]

		// For range queries, check all values
		for _, values := range series.Values {
			if len(values) >= 2 {
				if valueStr, ok := values[1].(string); ok && valueStr == "1" && alertState == alertStateFiring {
					return kuberikcomv1alpha1.HealthStatusUnhealthy, fmt.Sprintf("Alert firing for %s", formatAlertLabels(labels))
				}
			}
		}

		// For instant queries, check the single value
		if len(series.Value) >= 2 {
			if valueStr, ok := series.Value[1].(string); ok && valueStr == "1" && alertState == alertStateFiring {
				return kuberikcomv1alpha1.HealthStatusUnhealthy, fmt.Sprintf("Alert firing for %s", formatAlertLabels(labels))
			}
		}

		// If we have pending alerts but not firing, mark as pending
		if alertState == "pending" {
			return kuberikcomv1alpha1.HealthStatusPending, fmt.Sprintf("Alert pending for %s", formatAlertLabels(labels))
		}
	}

	return kuberikcomv1alpha1.HealthStatusHealthy, fmt.Sprintf("No alerts firing for %s", formatAlertLabels(labels))
}

// getLastScrapeTime retrieves the last scrape timestamp from the HealthCheck annotation
func (r *PrometheusAlertHealthCheckReconciler) getLastScrapeTime(healthCheck *kuberikcomv1alpha1.HealthCheck) time.Time {
	if healthCheck.Annotations == nil {
		return time.Time{}
	}

	lastScrapeStr, exists := healthCheck.Annotations[AnnotationLastScrapeTime]
	if !exists || lastScrapeStr == "" {
		return time.Time{}
	}

	t, err := time.Parse(time.RFC3339, lastScrapeStr)
	if err != nil {
		return time.Time{}
	}

	return t
}

// setLastScrapeTime updates the last scrape timestamp on the HealthCheck annotation
func (r *PrometheusAlertHealthCheckReconciler) setLastScrapeTime(ctx context.Context, healthCheck *kuberikcomv1alpha1.HealthCheck, t time.Time) error {
	if healthCheck.Annotations == nil {
		healthCheck.Annotations = make(map[string]string)
	}

	healthCheck.Annotations[AnnotationLastScrapeTime] = t.Format(time.RFC3339)
	return r.Update(ctx, healthCheck)
}

// updateHealthCheckStatus updates the HealthCheck status
func (r *PrometheusAlertHealthCheckReconciler) updateHealthCheckStatus(ctx context.Context, healthCheck *kuberikcomv1alpha1.HealthCheck, newStatus kuberikcomv1alpha1.HealthStatus, newMessage string, observations ...observation) (ctrl.Result, error) {
	now := metav1.Now()

	// Re-fetch to avoid conflicts after annotation update
	latest := &kuberikcomv1alpha1.HealthCheck{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: healthCheck.Namespace, Name: healthCheck.Name}, latest); err != nil {
		return ctrl.Result{}, err
	}

	// Check if status has changed
	statusChanged := latest.Status.Status != newStatus

	// Update status fields
	latest.Status.Status = newStatus
	latest.Status.Message = &newMessage

	// Update LastChangeTime only if status changed
	if statusChanged {
		latest.Status.LastChangeTime = &now
	}

	// Update LastErrorTime if unhealthy
	if newStatus == kuberikcomv1alpha1.HealthStatusUnhealthy {
		latest.Status.LastErrorTime = &now
	}

	// observations is nil on error paths: keep the previous ones then.
	if observations != nil {
		merged, _ := obs.Merge(latest.Status.Observations, obs.Sort(observations))
		latest.Status.Observations = merged
	}

	// Update the status
	if err := r.Status().Update(ctx, latest); err != nil {
		return ctrl.Result{}, err
	}

	// Set requeue interval
	requeueAfter := r.getRequeueInterval(healthCheck)

	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// getRequeueInterval extracts the requeue interval from HealthCheck annotations
func (r *PrometheusAlertHealthCheckReconciler) getRequeueInterval(healthCheck *kuberikcomv1alpha1.HealthCheck) time.Duration {
	defaultInterval := 30 * time.Second

	if healthCheck.Annotations == nil {
		return defaultInterval
	}

	if intervalStr, exists := healthCheck.Annotations[AnnotationRequeueInterval]; exists {
		if interval, err := time.ParseDuration(intervalStr); err == nil {
			// Ensure minimum interval of 5 seconds
			if interval < 5*time.Second {
				return 5 * time.Second
			}
			return interval
		}
	}

	return defaultInterval
}

// findHealthChecksForSecret maps Secret events to HealthCheck reconciliation requests
func (r *PrometheusAlertHealthCheckReconciler) findHealthChecksForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	var requests []reconcile.Request

	secret, ok := obj.(*corev1.Secret)
	if !ok {
		return requests
	}

	// List all HealthChecks to find ones that reference this Secret
	healthCheckList := &kuberikcomv1alpha1.HealthCheckList{}
	if err := r.List(ctx, healthCheckList); err != nil {
		logf.FromContext(ctx).Error(err, "failed to list HealthChecks")
		return requests
	}

	for _, healthCheck := range healthCheckList.Items {
		// Check if this is a prometheus-alert health check
		if healthCheck.Spec.Class == nil || *healthCheck.Spec.Class != HealthCheckClassPrometheusAlert {
			continue
		}

		// Check if this HealthCheck references the Secret
		if r.healthCheckReferencesSecret(&healthCheck, secret) {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: healthCheck.Namespace,
					Name:      healthCheck.Name,
				},
			})
		}
	}

	return requests
}

// healthCheckReferencesSecret checks if a HealthCheck references a specific Secret
func (r *PrometheusAlertHealthCheckReconciler) healthCheckReferencesSecret(healthCheck *kuberikcomv1alpha1.HealthCheck, secret *corev1.Secret) bool {
	if healthCheck.Annotations == nil {
		return false
	}

	secretRef, exists := healthCheck.Annotations[AnnotationPrometheusCredentialsSecret]
	if !exists {
		return false
	}

	// Parse secret reference
	secretNamespace := healthCheck.Namespace
	secretName := secretRef
	if parts := strings.Split(secretRef, "/"); len(parts) == 2 {
		secretNamespace = parts[0]
		secretName = parts[1]
	}

	return secretNamespace == secret.Namespace && secretName == secret.Name
}

// SetupWithManager sets up the controller with the Manager.
func (r *PrometheusAlertHealthCheckReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kuberikcomv1alpha1.HealthCheck{}, builder.WithPredicates(obs.IgnoreObservationOnlyUpdates())).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.findHealthChecksForSecret),
		).
		Named("prometheusalerthealthcheck").
		Complete(r)
}
