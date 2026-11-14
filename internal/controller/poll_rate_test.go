package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	kuberikcomv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
)

type countingQuerier struct {
	url   string
	calls atomic.Int32
	mu    sync.Mutex
	times []time.Time
	resp  *prometheusQueryResponse
}

func (c *countingQuerier) QueryAlerts(_ context.Context, url, _ string, _, _ time.Time, _ *prometheusAuth) (*prometheusQueryResponse, error) {
	// Objects left behind by other specs are reconciled too; count only ours.
	if url != c.url {
		return c.resp, nil
	}
	c.calls.Add(1)
	c.mu.Lock()
	c.times = append(c.times, time.Now())
	c.mu.Unlock()
	return c.resp, nil
}

var _ = Describe("poll rate under a running manager", func() {
	for _, state := range []string{"firing", "none"} {
		It("polls Prometheus no more often than requeue-interval ("+state+")", func() {
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "poll-ns-"}}
			Expect(k8sClient.Create(context.Background(), ns)).To(Succeed())

			mctx, stop := context.WithCancel(context.Background())
			defer stop()
			mgr, err := ctrl.NewManager(cfg, ctrl.Options{
				Scheme:                 scheme.Scheme,
				Metrics:                metricsserver.Options{BindAddress: "0"},
				HealthProbeBindAddress: "0",
				Controller:             config.Controller{SkipNameValidation: ptr.To(true)},
			})
			Expect(err).NotTo(HaveOccurred())
			q := &countingQuerier{url: "http://fake-" + ns.Name}
			if state == "firing" {
				q.resp = pollResponse(`[{"metric":{"alertname":"A","alertstate":"firing"},"values":[[1,"1"]]}]`)
			} else {
				q.resp = pollResponse(`[]`)
			}
			Expect((&PrometheusAlertHealthCheckReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), AlertQuerier: q}).SetupWithManager(mgr)).To(Succeed())
			go func() { defer GinkgoRecover(); _ = mgr.Start(mctx) }()

			hc := &kuberikcomv1alpha1.HealthCheck{
				ObjectMeta: metav1.ObjectMeta{Name: "poll", Namespace: ns.Name, Annotations: map[string]string{
					AnnotationPrometheusURL:         "http://fake-" + ns.Name,
					AnnotationPrometheusAlertLabels: "alertname=A",
					AnnotationRequeueInterval:       "5s",
				}},
				Spec: kuberikcomv1alpha1.HealthCheckSpec{Class: ptr.To(HealthCheckClassPrometheusAlert)},
			}
			Expect(k8sClient.Create(context.Background(), hc)).To(Succeed())
			// Other specs' managers must not keep polling this object.
			defer func() { Expect(client.IgnoreNotFound(k8sClient.Delete(context.Background(), hc))).To(Succeed()) }()

			// 12s at a 5s interval allows at most 3 polls (t=0, 5, 10).
			time.Sleep(12 * time.Second)
			Expect(int(q.calls.Load())).To(BeNumerically("<=", 3), "polled %d times in 12s with requeue-interval=5s: %v", q.calls.Load(), q.times)
			Expect(int(q.calls.Load())).To(BeNumerically(">=", 1))
		})
	}
})

func pollResponse(result string) *prometheusQueryResponse {
	res := &prometheusQueryResponse{}
	body := fmt.Sprintf(`{"status":"success","data":{"resultType":"matrix","result":%s}}`, result)
	Expect(json.Unmarshal([]byte(body), res)).To(Succeed())
	return res
}
