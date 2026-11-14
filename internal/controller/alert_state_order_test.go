package controller

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	kuberikcomv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

func matrix(t *testing.T, series ...string) *prometheusQueryResponse {
	t.Helper()
	body := fmt.Sprintf(`{"status":"success","data":{"resultType":"matrix","result":[%s]}}`, strings.Join(series, ","))
	res := &prometheusQueryResponse{}
	if err := json.Unmarshal([]byte(body), res); err != nil {
		t.Fatal(err)
	}
	return res
}

func ser(state, value string) string {
	return fmt.Sprintf(`{"metric":{"alertname":"A","alertstate":"%s"},"values":[[1,"%s"]]}`, state, value)
}

// The worst state across every matching series wins, whatever order
// Prometheus lists them in.
func TestEvaluateAlertResultWorstStateWinsInAnyOrder(t *testing.T) {
	r := &PrometheusAlertHealthCheckReconciler{}
	labels := map[string]string{"alertname": "A"}
	cases := []struct {
		name   string
		series []string
		want   kuberikcomv1alpha1.HealthStatus
	}{
		{"pending then firing", []string{ser("pending", "1"), ser("firing", "1")}, kuberikcomv1alpha1.HealthStatusUnhealthy},
		{"firing then pending", []string{ser("firing", "1"), ser("pending", "1")}, kuberikcomv1alpha1.HealthStatusUnhealthy},
		{"pending then resolved firing", []string{ser("pending", "1"), ser("firing", "0")}, kuberikcomv1alpha1.HealthStatusPending},
		{"pending only", []string{ser("pending", "1")}, kuberikcomv1alpha1.HealthStatusPending},
		{"nothing active", []string{ser("firing", "0")}, kuberikcomv1alpha1.HealthStatusHealthy},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got, _ := r.evaluateAlertResult(matrix(t, c.series...), labels); got != c.want {
				t.Fatalf("got %s want %s", got, c.want)
			}
		})
	}
}
