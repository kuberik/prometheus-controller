package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/event"

	kuberikcomv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

func TestIgnoreOwnBookkeepingUpdates(t *testing.T) {
	base := func() *kuberikcomv1alpha1.HealthCheck {
		return &kuberikcomv1alpha1.HealthCheck{
			ObjectMeta: metav1.ObjectMeta{Generation: 1, Annotations: map[string]string{AnnotationRequeueInterval: "5s"}},
			Status:     kuberikcomv1alpha1.HealthCheckStatus{Status: kuberikcomv1alpha1.HealthStatusHealthy, Message: ptr.To("ok")},
		}
	}
	cases := []struct {
		name   string
		mutate func(*kuberikcomv1alpha1.HealthCheck)
		want   bool
	}{
		{"scrape annotation only", func(h *kuberikcomv1alpha1.HealthCheck) {
			h.Annotations[AnnotationLastScrapeTime] = "2026-01-01T00:00:00Z"
		}, false},
		{"lastErrorTime refresh only", func(h *kuberikcomv1alpha1.HealthCheck) { h.Status.LastErrorTime = ptr.To(metav1.Now()) }, false},
		{"other annotation", func(h *kuberikcomv1alpha1.HealthCheck) { h.Annotations[AnnotationRequeueInterval] = "10s" }, true},
		{"spec generation", func(h *kuberikcomv1alpha1.HealthCheck) { h.Generation = 2 }, true},
		{"status reset by rollout-controller", func(h *kuberikcomv1alpha1.HealthCheck) {
			h.Status.Status = kuberikcomv1alpha1.HealthStatusPending
			h.Status.Message = ptr.To("Health check reset due to new deployment")
		}, true},
	}
	p := ignoreOwnBookkeepingUpdates()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o, n := base(), base()
			c.mutate(n)
			if got := p.Update(event.UpdateEvent{ObjectOld: o, ObjectNew: n}); got != c.want {
				t.Fatalf("got %v want %v", got, c.want)
			}
		})
	}
}
