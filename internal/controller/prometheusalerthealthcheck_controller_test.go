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
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kuberikcomv1alpha1 "github.com/kuberik/rollout-controller/api/v1alpha1"
	obs "github.com/kuberik/rollout-controller/pkg/healthcheck/observation"
)

// MockAlertQuerier is a mock implementation of AlertQuerier for testing
type MockAlertQuerier struct {
	Response *prometheusQueryResponse
	Error    error
}

func (m *MockAlertQuerier) QueryAlerts(ctx context.Context, prometheusURL, query string, start, end time.Time, auth *prometheusAuth) (*prometheusQueryResponse, error) {
	if m.Error != nil {
		return nil, m.Error
	}
	return m.Response, nil
}

var _ = Describe("PrometheusAlertHealthCheck observations", func() {
	var (
		ctx        = context.Background()
		server     *httptest.Server
		rulesJSON  string
		namespace  string
		hcName     = "obs-healthcheck"
		reconciler *PrometheusAlertHealthCheckReconciler
	)

	BeforeEach(func() {
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case "/api/v1/query_range":
				_, _ = fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"alertname":"HighErrorRate","alertstate":"firing","severity":"critical"},"values":[[1,"1"]]}]}}`)
			case "/api/v1/rules":
				Expect(r.URL.Query().Get("type")).To(Equal("alert"))
				_, _ = fmt.Fprint(w, rulesJSON)
			default:
				http.NotFound(w, r)
			}
		}))
		rulesJSON = `{"status":"success","data":{"groups":[{"rules":[{"name":"HighErrorRate","query":"rate(errors[5m]) > 0.05","type":"alerting","labels":{"severity":"critical"},"alerts":[{"labels":{"alertname":"HighErrorRate","severity":"critical","pod":"a"},"state":"firing","value":"1.2e-01"},{"labels":{"alertname":"HighErrorRate","severity":"critical","pod":"b"},"state":"firing","value":"7e-02"}]}]}]}}`
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "obs-ns-"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		namespace = ns.Name
		reconciler = &PrometheusAlertHealthCheckReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), AlertQuerier: &DefaultAlertQuerier{}}
	})

	AfterEach(func() {
		server.Close()
		Expect(k8sClient.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	reconcileWith := func(extra map[string]string) *kuberikcomv1alpha1.HealthCheck {
		annotations := map[string]string{
			AnnotationPrometheusURL:         server.URL,
			AnnotationPrometheusAlertLabels: "alertname=HighErrorRate,severity=critical",
		}
		for k, v := range extra {
			annotations[k] = v
		}
		hc := &kuberikcomv1alpha1.HealthCheck{
			ObjectMeta: metav1.ObjectMeta{Name: hcName, Namespace: namespace, Annotations: annotations},
			Spec:       kuberikcomv1alpha1.HealthCheckSpec{Class: ptr.To(HealthCheckClassPrometheusAlert)},
		}
		Expect(k8sClient.Create(ctx, hc)).To(Succeed())
		key := types.NamespacedName{Name: hcName, Namespace: namespace}
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, key, hc)).To(Succeed())
		return hc
	}

	It("writes a Threshold with the worst evaluated value when the limit is annotated", func() {
		hc := reconcileWith(map[string]string{
			AnnotationObservationLimit: "0.05", AnnotationObservationDirection: "Above", AnnotationObservationUnit: "Ratio",
		})
		Expect(hc.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatusUnhealthy))
		Expect(hc.Status.Observations).To(HaveLen(1))
		o := hc.Status.Observations[0]
		Expect(o.Name).To(Equal("HighErrorRate"))
		Expect(o.Kind).To(Equal(kuberikcomv1alpha1.ObservationKindThreshold))
		Expect(o.Value).To(Equal("0.12"))
		Expect(o.Limit).To(Equal("0.05"))
		Expect(o.Direction).To(Equal(kuberikcomv1alpha1.ObservationDirectionAbove))
		Expect(o.Unit).To(Equal("Ratio"))
		Expect(*o.Healthy).To(BeFalse())
		Expect(o.ObservedTime).NotTo(BeNil())
	})

	It("writes a State named by alertname when the rule is not a trivial threshold", func() {
		rulesJSON = `{"status":"success","data":{"groups":[{"rules":[{"name":"HighErrorRate","query":"a > 1 and b > 2","type":"alerting","labels":{"severity":"critical"},"alerts":[{"labels":{"alertname":"HighErrorRate","severity":"critical"},"state":"firing","value":"1e+00"}]}]}]}}`
		hc := reconcileWith(nil)
		Expect(hc.Status.Observations).To(HaveLen(1))
		o := hc.Status.Observations[0]
		Expect(o.Name).To(Equal("HighErrorRate"))
		Expect(o.Kind).To(Equal(kuberikcomv1alpha1.ObservationKindState))
		Expect(*o.Healthy).To(BeFalse())
		Expect(o.Message).To(BeEmpty())
	})

	It("infers the Threshold from a simple rule expression without annotations", func() {
		hc := reconcileWith(nil)
		o := hc.Status.Observations[0]
		Expect(o.Kind).To(Equal(kuberikcomv1alpha1.ObservationKindThreshold))
		Expect(o.Value).To(Equal("0.12"))
		Expect(o.Limit).To(Equal("0.05"))
	})

	It("keeps observedTime stable across an unchanged re-reconcile", func() {
		hc := reconcileWith(nil)
		first := hc.Status.Observations[0].ObservedTime.DeepCopy()
		time.Sleep(1100 * time.Millisecond)
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: hcName, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: hcName, Namespace: namespace}, hc)).To(Succeed())
		Expect(hc.Status.Observations[0].ObservedTime.Equal(first)).To(BeTrue())
	})

	It("still reports status and a Count when the rules API fails", func() {
		server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/api/v1/rules" {
				http.Error(w, "nope", http.StatusInternalServerError)
				return
			}
			_, _ = fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
		})
		hc := reconcileWith(nil)
		Expect(hc.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatusHealthy))
		Expect(hc.Status.Observations).To(HaveLen(1))
		Expect(hc.Status.Observations[0].Kind).To(Equal(kuberikcomv1alpha1.ObservationKindCount))
		Expect(hc.Status.Observations[0].Value).To(Equal("0"))
	})
})

var _ = Describe("PrometheusAlertHealthCheck Controller", func() {
	Context("When reconciling a HealthCheck with class prometheus-alert", func() {
		const resourceName = "test-prometheus-healthcheck"

		var namespace string
		var typeNamespacedName types.NamespacedName
		var healthCheck *kuberikcomv1alpha1.HealthCheck
		var mockQuerier *MockAlertQuerier

		JustBeforeEach(func() {
			ctx := context.Background()
			By("creating a unique namespace for the test")
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "test-ns-",
				},
			}
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())
			namespace = ns.Name

			By("setting up the test environment")
			typeNamespacedName = types.NamespacedName{
				Name:      resourceName,
				Namespace: namespace,
			}

			By("creating the HealthCheck with class prometheus-alert")
			healthCheck = &kuberikcomv1alpha1.HealthCheck{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: namespace,
					Annotations: map[string]string{
						AnnotationPrometheusURL:         "http://prometheus:9090",
						AnnotationPrometheusAlertLabels: "alertname=HighErrorRate,severity=critical",
					},
				},
				Spec: kuberikcomv1alpha1.HealthCheckSpec{
					Class: ptr.To(HealthCheckClassPrometheusAlert),
				},
			}
			Expect(k8sClient.Create(ctx, healthCheck)).To(Succeed())

			By("initializing the mock querier")
			mockQuerier = &MockAlertQuerier{}
		})

		AfterEach(func() {
			ctx := context.Background()
			By("Cleaning up the test namespace")
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: namespace,
				},
			}
			Expect(k8sClient.Delete(ctx, ns)).To(Succeed())
		})

		It("should update HealthCheck status to Healthy when no alerts are firing", func() {
			ctx := context.Background()
			By("setting up mock to return empty result")
			mockQuerier.Response = &prometheusQueryResponse{
				Status: "success",
				Data: struct {
					ResultType string `json:"resultType"`
					Result     []struct {
						Metric map[string]string `json:"metric"`
						Value  []interface{}     `json:"value"`
						Values [][]interface{}   `json:"values"`
					} `json:"result"`
				}{
					ResultType: "matrix",
					Result:     nil,
				},
			}

			By("Reconciling the HealthCheck")
			controllerReconciler := &PrometheusAlertHealthCheckReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				AlertQuerier: mockQuerier,
			}
			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0))

			By("Verifying that HealthCheck status was updated to Healthy")
			updatedHealthCheck := &kuberikcomv1alpha1.HealthCheck{}
			err = k8sClient.Get(ctx, typeNamespacedName, updatedHealthCheck)
			Expect(err).NotTo(HaveOccurred())

			Expect(updatedHealthCheck.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatusHealthy))
			Expect(updatedHealthCheck.Status.Message).NotTo(BeNil())
			Expect(*updatedHealthCheck.Status.Message).To(ContainSubstring("No alerts firing"))
		})

		It("should update HealthCheck status to Unhealthy when alert is firing", func() {
			ctx := context.Background()
			By("setting up mock to return firing alert")
			mockQuerier.Response = &prometheusQueryResponse{
				Status: "success",
				Data: struct {
					ResultType string `json:"resultType"`
					Result     []struct {
						Metric map[string]string `json:"metric"`
						Value  []interface{}     `json:"value"`
						Values [][]interface{}   `json:"values"`
					} `json:"result"`
				}{
					ResultType: "matrix",
					Result: []struct {
						Metric map[string]string `json:"metric"`
						Value  []interface{}     `json:"value"`
						Values [][]interface{}   `json:"values"`
					}{
						{
							Metric: map[string]string{
								"alertname":  "HighErrorRate",
								"alertstate": "firing",
								"severity":   "critical",
							},
							Values: [][]interface{}{
								{float64(1234567890), "1"},
							},
						},
					},
				},
			}

			By("Reconciling the HealthCheck")
			controllerReconciler := &PrometheusAlertHealthCheckReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				AlertQuerier: mockQuerier,
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that HealthCheck status was updated to Unhealthy")
			updatedHealthCheck := &kuberikcomv1alpha1.HealthCheck{}
			err = k8sClient.Get(ctx, typeNamespacedName, updatedHealthCheck)
			Expect(err).NotTo(HaveOccurred())

			Expect(updatedHealthCheck.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatusUnhealthy))
			Expect(updatedHealthCheck.Status.Message).NotTo(BeNil())
			Expect(*updatedHealthCheck.Status.Message).To(ContainSubstring("Alert firing"))
			Expect(updatedHealthCheck.Status.LastErrorTime).NotTo(BeNil())

			By("recording the firing alert as a Critical failure")
			Expect(updatedHealthCheck.Status.Failures).To(HaveLen(1))
			f := updatedHealthCheck.Status.Failures[0]
			Expect(f.Source).To(Equal("Alert HighErrorRate"))
			Expect(f.Kind).To(Equal(kuberikcomv1alpha1.FailureKindAlert))
			Expect(f.Severity).To(Equal(kuberikcomv1alpha1.FailureSeverityCritical))

			By("keeping the failure (witness) after the alert recovers")
			mockQuerier.Response = &prometheusQueryResponse{Status: "success"}
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, typeNamespacedName, updatedHealthCheck)).To(Succeed())
			Expect(updatedHealthCheck.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatusHealthy))
			Expect(updatedHealthCheck.Status.Failures).To(HaveLen(1))
			Expect(updatedHealthCheck.Status.Failures[0].FirstSeenTime).To(Equal(f.FirstSeenTime))
		})

		It("should update HealthCheck status to Pending when alert is pending", func() {
			ctx := context.Background()
			By("setting up mock to return pending alert")
			mockQuerier.Response = &prometheusQueryResponse{
				Status: "success",
				Data: struct {
					ResultType string `json:"resultType"`
					Result     []struct {
						Metric map[string]string `json:"metric"`
						Value  []interface{}     `json:"value"`
						Values [][]interface{}   `json:"values"`
					} `json:"result"`
				}{
					ResultType: "matrix",
					Result: []struct {
						Metric map[string]string `json:"metric"`
						Value  []interface{}     `json:"value"`
						Values [][]interface{}   `json:"values"`
					}{
						{
							Metric: map[string]string{
								"alertname":  "HighErrorRate",
								"alertstate": "pending",
								"severity":   "critical",
							},
							Values: [][]interface{}{
								{float64(1234567890), "1"},
							},
						},
					},
				},
			}

			By("Reconciling the HealthCheck")
			controllerReconciler := &PrometheusAlertHealthCheckReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				AlertQuerier: mockQuerier,
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that HealthCheck status was updated to Pending")
			updatedHealthCheck := &kuberikcomv1alpha1.HealthCheck{}
			err = k8sClient.Get(ctx, typeNamespacedName, updatedHealthCheck)
			Expect(err).NotTo(HaveOccurred())

			Expect(updatedHealthCheck.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatusPending))
			Expect(*updatedHealthCheck.Status.Message).To(ContainSubstring("Alert pending"))
		})

		It("should update HealthCheck to Pending when Prometheus query fails", func() {
			ctx := context.Background()
			By("setting up mock to return error")
			mockQuerier.Error = fmt.Errorf("connection refused")

			By("Reconciling the HealthCheck")
			controllerReconciler := &PrometheusAlertHealthCheckReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				AlertQuerier: mockQuerier,
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that HealthCheck status was updated to Unhealthy")
			updatedHealthCheck := &kuberikcomv1alpha1.HealthCheck{}
			err = k8sClient.Get(ctx, typeNamespacedName, updatedHealthCheck)
			Expect(err).NotTo(HaveOccurred())

			Expect(updatedHealthCheck.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatusPending))
			Expect(updatedHealthCheck.Status.LastErrorTime).To(BeNil())
			Expect(updatedHealthCheck.Status.Observations).To(BeEmpty())
			Expect(updatedHealthCheck.Status.Message).NotTo(BeNil())
			Expect(*updatedHealthCheck.Status.Message).To(ContainSubstring("connection refused"))
		})

		It("should skip HealthChecks without class annotation", func() {
			ctx := context.Background()
			By("creating a HealthCheck without class")
			noClassHealthCheck := &kuberikcomv1alpha1.HealthCheck{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-class-healthcheck",
					Namespace: namespace,
				},
			}
			Expect(k8sClient.Create(ctx, noClassHealthCheck)).To(Succeed())

			By("Reconciling the HealthCheck")
			controllerReconciler := &PrometheusAlertHealthCheckReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				AlertQuerier: mockQuerier,
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      "no-class-healthcheck",
					Namespace: namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that the HealthCheck status was not changed")
			updatedHealthCheck := &kuberikcomv1alpha1.HealthCheck{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      "no-class-healthcheck",
				Namespace: namespace,
			}, updatedHealthCheck)
			Expect(err).NotTo(HaveOccurred())

			Expect(updatedHealthCheck.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatus("")))
		})

		It("should skip HealthChecks with different class", func() {
			ctx := context.Background()
			By("creating a HealthCheck with different class")
			otherClassHealthCheck := &kuberikcomv1alpha1.HealthCheck{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "other-class-healthcheck",
					Namespace: namespace,
				},
				Spec: kuberikcomv1alpha1.HealthCheckSpec{
					Class: ptr.To("other-class"),
				},
			}
			Expect(k8sClient.Create(ctx, otherClassHealthCheck)).To(Succeed())

			By("Reconciling the HealthCheck")
			controllerReconciler := &PrometheusAlertHealthCheckReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				AlertQuerier: mockQuerier,
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      "other-class-healthcheck",
					Namespace: namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that the HealthCheck status was not changed")
			updatedHealthCheck := &kuberikcomv1alpha1.HealthCheck{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      "other-class-healthcheck",
				Namespace: namespace,
			}, updatedHealthCheck)
			Expect(err).NotTo(HaveOccurred())

			Expect(updatedHealthCheck.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatus("")))
		})

		It("should set Pending status when Prometheus URL annotation is missing", func() {
			ctx := context.Background()
			By("creating a HealthCheck without Prometheus URL annotation")
			noURLHealthCheck := &kuberikcomv1alpha1.HealthCheck{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-url-healthcheck",
					Namespace: namespace,
					Annotations: map[string]string{
						AnnotationPrometheusAlertLabels: "alertname=Test",
					},
				},
				Spec: kuberikcomv1alpha1.HealthCheckSpec{
					Class: ptr.To(HealthCheckClassPrometheusAlert),
				},
			}
			Expect(k8sClient.Create(ctx, noURLHealthCheck)).To(Succeed())

			By("Reconciling the HealthCheck")
			controllerReconciler := &PrometheusAlertHealthCheckReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				AlertQuerier: mockQuerier,
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      "no-url-healthcheck",
					Namespace: namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that HealthCheck status was updated to Unhealthy")
			updatedHealthCheck := &kuberikcomv1alpha1.HealthCheck{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      "no-url-healthcheck",
				Namespace: namespace,
			}, updatedHealthCheck)
			Expect(err).NotTo(HaveOccurred())

			Expect(updatedHealthCheck.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatusPending))
			Expect(updatedHealthCheck.Status.LastErrorTime).To(BeNil())
			Expect(updatedHealthCheck.Status.Observations).To(BeEmpty())
			Expect(*updatedHealthCheck.Status.Message).To(ContainSubstring("prometheus-url"))
		})

		It("should set Pending status when alert labels annotation is missing", func() {
			ctx := context.Background()
			By("creating a HealthCheck without alert labels annotation")
			noLabelsHealthCheck := &kuberikcomv1alpha1.HealthCheck{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-labels-healthcheck",
					Namespace: namespace,
					Annotations: map[string]string{
						AnnotationPrometheusURL: "http://prometheus:9090",
					},
				},
				Spec: kuberikcomv1alpha1.HealthCheckSpec{
					Class: ptr.To(HealthCheckClassPrometheusAlert),
				},
			}
			Expect(k8sClient.Create(ctx, noLabelsHealthCheck)).To(Succeed())

			By("Reconciling the HealthCheck")
			controllerReconciler := &PrometheusAlertHealthCheckReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				AlertQuerier: mockQuerier,
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      "no-labels-healthcheck",
					Namespace: namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that HealthCheck status was updated to Unhealthy")
			updatedHealthCheck := &kuberikcomv1alpha1.HealthCheck{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      "no-labels-healthcheck",
				Namespace: namespace,
			}, updatedHealthCheck)
			Expect(err).NotTo(HaveOccurred())

			Expect(updatedHealthCheck.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatusPending))
			Expect(updatedHealthCheck.Status.LastErrorTime).To(BeNil())
			Expect(updatedHealthCheck.Status.Observations).To(BeEmpty())
			Expect(*updatedHealthCheck.Status.Message).To(ContainSubstring("prometheus-alert-labels"))
		})

		It("should respect custom requeue interval from annotation", func() {
			ctx := context.Background()
			By("updating the HealthCheck with custom requeue interval")
			err := k8sClient.Get(ctx, typeNamespacedName, healthCheck)
			Expect(err).NotTo(HaveOccurred())

			healthCheck.Annotations[AnnotationRequeueInterval] = "1m"
			Expect(k8sClient.Update(ctx, healthCheck)).To(Succeed())

			By("setting up mock to return empty result")
			mockQuerier.Response = &prometheusQueryResponse{
				Status: "success",
				Data: struct {
					ResultType string `json:"resultType"`
					Result     []struct {
						Metric map[string]string `json:"metric"`
						Value  []interface{}     `json:"value"`
						Values [][]interface{}   `json:"values"`
					} `json:"result"`
				}{
					ResultType: "matrix",
					Result:     nil,
				},
			}

			By("Reconciling the HealthCheck")
			controllerReconciler := &PrometheusAlertHealthCheckReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				AlertQuerier: mockQuerier,
			}
			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that requeue interval is 1 minute")
			Expect(result.RequeueAfter.Minutes()).To(BeNumerically("~", 1, 0.01))
		})

		It("should enforce minimum requeue interval of 5 seconds", func() {
			ctx := context.Background()
			By("updating the HealthCheck with too short requeue interval")
			err := k8sClient.Get(ctx, typeNamespacedName, healthCheck)
			Expect(err).NotTo(HaveOccurred())

			healthCheck.Annotations[AnnotationRequeueInterval] = "1s"
			Expect(k8sClient.Update(ctx, healthCheck)).To(Succeed())

			By("setting up mock to return empty result")
			mockQuerier.Response = &prometheusQueryResponse{
				Status: "success",
				Data: struct {
					ResultType string `json:"resultType"`
					Result     []struct {
						Metric map[string]string `json:"metric"`
						Value  []interface{}     `json:"value"`
						Values [][]interface{}   `json:"values"`
					} `json:"result"`
				}{
					ResultType: "matrix",
					Result:     nil,
				},
			}

			By("Reconciling the HealthCheck")
			controllerReconciler := &PrometheusAlertHealthCheckReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				AlertQuerier: mockQuerier,
			}
			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that requeue interval is enforced to minimum 5 seconds")
			Expect(result.RequeueAfter.Seconds()).To(BeNumerically(">=", 5))
		})

		It("should store and use last scrape time", func() {
			ctx := context.Background()
			By("setting up mock to return empty result")
			mockQuerier.Response = &prometheusQueryResponse{
				Status: "success",
				Data: struct {
					ResultType string `json:"resultType"`
					Result     []struct {
						Metric map[string]string `json:"metric"`
						Value  []interface{}     `json:"value"`
						Values [][]interface{}   `json:"values"`
					} `json:"result"`
				}{
					ResultType: "matrix",
					Result:     nil,
				},
			}

			By("Reconciling the HealthCheck for the first time")
			controllerReconciler := &PrometheusAlertHealthCheckReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				AlertQuerier: mockQuerier,
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that last scrape time was set")
			updatedHealthCheck := &kuberikcomv1alpha1.HealthCheck{}
			err = k8sClient.Get(ctx, typeNamespacedName, updatedHealthCheck)
			Expect(err).NotTo(HaveOccurred())

			lastScrapeStr, exists := updatedHealthCheck.Annotations[AnnotationLastScrapeTime]
			Expect(exists).To(BeTrue())
			Expect(lastScrapeStr).NotTo(BeEmpty())

			lastScrape, err := time.Parse(time.RFC3339, lastScrapeStr)
			Expect(err).NotTo(HaveOccurred())
			Expect(lastScrape).To(BeTemporally("~", time.Now(), 5*time.Second))
		})

		It("should handle credentials with bearer token", func() {
			ctx := context.Background()
			By("creating a secret with bearer token")
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "prom-credentials",
					Namespace: namespace,
				},
				Data: map[string][]byte{
					"token": []byte("test-bearer-token"),
				},
			}
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())

			By("updating the HealthCheck with credentials reference")
			err := k8sClient.Get(ctx, typeNamespacedName, healthCheck)
			Expect(err).NotTo(HaveOccurred())
			healthCheck.Annotations[AnnotationPrometheusCredentialsSecret] = "prom-credentials"
			Expect(k8sClient.Update(ctx, healthCheck)).To(Succeed())

			By("setting up mock to return empty result")
			mockQuerier.Response = &prometheusQueryResponse{
				Status: "success",
				Data: struct {
					ResultType string `json:"resultType"`
					Result     []struct {
						Metric map[string]string `json:"metric"`
						Value  []interface{}     `json:"value"`
						Values [][]interface{}   `json:"values"`
					} `json:"result"`
				}{
					ResultType: "matrix",
					Result:     nil,
				},
			}

			By("Reconciling the HealthCheck")
			controllerReconciler := &PrometheusAlertHealthCheckReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				AlertQuerier: mockQuerier,
			}
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that HealthCheck status was updated to Healthy")
			updatedHealthCheck := &kuberikcomv1alpha1.HealthCheck{}
			err = k8sClient.Get(ctx, typeNamespacedName, updatedHealthCheck)
			Expect(err).NotTo(HaveOccurred())
			Expect(updatedHealthCheck.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatusHealthy))
		})

		It("should set Pending when credentials secret does not exist", func() {
			ctx := context.Background()
			By("updating the HealthCheck with reference to non-existent secret")
			err := k8sClient.Get(ctx, typeNamespacedName, healthCheck)
			Expect(err).NotTo(HaveOccurred())
			healthCheck.Annotations[AnnotationPrometheusCredentialsSecret] = "non-existent-secret"
			Expect(k8sClient.Update(ctx, healthCheck)).To(Succeed())

			By("Reconciling the HealthCheck")
			controllerReconciler := &PrometheusAlertHealthCheckReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				AlertQuerier: mockQuerier,
			}
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that HealthCheck status was updated to Unhealthy")
			updatedHealthCheck := &kuberikcomv1alpha1.HealthCheck{}
			err = k8sClient.Get(ctx, typeNamespacedName, updatedHealthCheck)
			Expect(err).NotTo(HaveOccurred())

			Expect(updatedHealthCheck.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatusPending))
			Expect(updatedHealthCheck.Status.LastErrorTime).To(BeNil())
			Expect(updatedHealthCheck.Status.Observations).To(BeEmpty())
			Expect(*updatedHealthCheck.Status.Message).To(ContainSubstring("non-existent-secret"))
		})

		It("should detect firing alert across multiple time series points in range", func() {
			ctx := context.Background()
			By("setting up mock with multiple time series points where later ones are firing")
			mockQuerier.Response = &prometheusQueryResponse{
				Status: "success",
				Data: struct {
					ResultType string `json:"resultType"`
					Result     []struct {
						Metric map[string]string `json:"metric"`
						Value  []interface{}     `json:"value"`
						Values [][]interface{}   `json:"values"`
					} `json:"result"`
				}{
					ResultType: "matrix",
					Result: []struct {
						Metric map[string]string `json:"metric"`
						Value  []interface{}     `json:"value"`
						Values [][]interface{}   `json:"values"`
					}{
						{
							Metric: map[string]string{
								"alertname":  "HighErrorRate",
								"alertstate": "firing",
								"severity":   "critical",
							},
							Values: [][]interface{}{
								{float64(1234567880), "0"},
								{float64(1234567890), "1"},
								{float64(1234567900), "1"},
							},
						},
					},
				},
			}

			By("Reconciling the HealthCheck")
			controllerReconciler := &PrometheusAlertHealthCheckReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				AlertQuerier: mockQuerier,
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying that HealthCheck status was updated to Unhealthy")
			updatedHealthCheck := &kuberikcomv1alpha1.HealthCheck{}
			err = k8sClient.Get(ctx, typeNamespacedName, updatedHealthCheck)
			Expect(err).NotTo(HaveOccurred())

			Expect(updatedHealthCheck.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatusUnhealthy))
		})

		It("should update LastChangeTime when status changes", func() {
			ctx := context.Background()
			By("setting up mock to return empty result (healthy)")
			mockQuerier.Response = &prometheusQueryResponse{
				Status: "success",
				Data: struct {
					ResultType string `json:"resultType"`
					Result     []struct {
						Metric map[string]string `json:"metric"`
						Value  []interface{}     `json:"value"`
						Values [][]interface{}   `json:"values"`
					} `json:"result"`
				}{
					ResultType: "matrix",
					Result:     nil,
				},
			}

			By("Reconciling the HealthCheck")
			controllerReconciler := &PrometheusAlertHealthCheckReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				AlertQuerier: mockQuerier,
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying LastChangeTime was set")
			updatedHealthCheck := &kuberikcomv1alpha1.HealthCheck{}
			err = k8sClient.Get(ctx, typeNamespacedName, updatedHealthCheck)
			Expect(err).NotTo(HaveOccurred())
			Expect(updatedHealthCheck.Status.LastChangeTime).NotTo(BeNil())
			firstChangeTime := updatedHealthCheck.Status.LastChangeTime

			By("Changing to firing state")
			mockQuerier.Response = &prometheusQueryResponse{
				Status: "success",
				Data: struct {
					ResultType string `json:"resultType"`
					Result     []struct {
						Metric map[string]string `json:"metric"`
						Value  []interface{}     `json:"value"`
						Values [][]interface{}   `json:"values"`
					} `json:"result"`
				}{
					ResultType: "matrix",
					Result: []struct {
						Metric map[string]string `json:"metric"`
						Value  []interface{}     `json:"value"`
						Values [][]interface{}   `json:"values"`
					}{
						{
							Metric: map[string]string{
								"alertname":  "HighErrorRate",
								"alertstate": "firing",
							},
							Values: [][]interface{}{
								{float64(1234567890), "1"},
							},
						},
					},
				},
			}

			By("Reconciling again")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying LastChangeTime was updated")
			err = k8sClient.Get(ctx, typeNamespacedName, updatedHealthCheck)
			Expect(err).NotTo(HaveOccurred())
			Expect(updatedHealthCheck.Status.LastChangeTime).NotTo(BeNil())
			Expect(updatedHealthCheck.Status.LastChangeTime.Time).To(BeTemporally(">=", firstChangeTime.Time))
		})
	})
})

var _ = Describe("buildAlertsQuery", func() {
	It("should build query with single label", func() {
		query := buildAlertsQuery(map[string]string{"alertname": "MyAlert"})
		Expect(query).To(Equal(`ALERTS{alertname="MyAlert"}`))
	})

	It("should build query with multiple labels", func() {
		query := buildAlertsQuery(map[string]string{"alertname": "MyAlert", "severity": "critical"})
		Expect(query).To(ContainSubstring(`alertname="MyAlert"`))
		Expect(query).To(ContainSubstring(`severity="critical"`))
		Expect(query).To(HavePrefix("ALERTS{"))
		Expect(query).To(HaveSuffix("}"))
	})
})

var _ = Describe("PrometheusAlertHealthCheck per-alert observations", func() {
	var (
		ctx       = context.Background()
		namespace string
		querier   *fakeSeriesQuerier
		hcKey     types.NamespacedName
	)

	BeforeEach(func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "series-ns-"}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		namespace = ns.Name
		querier = &fakeSeriesQuerier{}
		hcKey = types.NamespacedName{Name: "series", Namespace: namespace}
	})

	AfterEach(func() {
		Expect(k8sClient.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	reconcileOnce := func(selector string) *kuberikcomv1alpha1.HealthCheck {
		hc := &kuberikcomv1alpha1.HealthCheck{}
		if err := k8sClient.Get(ctx, hcKey, hc); err != nil {
			hc = &kuberikcomv1alpha1.HealthCheck{
				ObjectMeta: metav1.ObjectMeta{Name: hcKey.Name, Namespace: namespace, Annotations: map[string]string{
					AnnotationPrometheusURL: "http://prometheus.invalid", AnnotationPrometheusAlertLabels: selector,
				}},
				Spec: kuberikcomv1alpha1.HealthCheckSpec{Class: ptr.To(HealthCheckClassPrometheusAlert)},
			}
			Expect(k8sClient.Create(ctx, hc)).To(Succeed())
		}
		r := &PrometheusAlertHealthCheckReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), AlertQuerier: querier}
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: hcKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, hcKey, hc)).To(Succeed())
		return hc
	}

	It("writes one observation per firing alert, named by alertname, with the distinguishing labels as message", func() {
		querier.series = []map[string]string{
			{"alertname": "HighErrorRate", "alertstate": "firing", "severity": "critical", "pod": "checkout-1"},
			{"alertname": "HighErrorRate", "alertstate": "firing", "severity": "critical", "pod": "checkout-2"},
			{"alertname": "DiskFull", "alertstate": "firing", "severity": "critical", "instance": "node-a"},
		}
		hc := reconcileOnce("severity=critical")
		Expect(hc.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatusUnhealthy))
		Expect(hc.Status.LastErrorTime).NotTo(BeNil())
		got := make([]string, 0, len(hc.Status.Observations))
		for _, o := range hc.Status.Observations {
			Expect(o.Healthy).NotTo(BeNil())
			Expect(*o.Healthy).To(BeFalse())
			got = append(got, o.Name+"|"+o.Message)
		}
		Expect(got).To(Equal([]string{
			"DiskFull|instance=node-a",
			"HighErrorRate|pod=checkout-1",
			"HighErrorRate|pod=checkout-2",
		}))
	})

	It("leaves healthy unset for a pending alert", func() {
		querier.series = []map[string]string{{"alertname": "HighErrorRate", "alertstate": "pending", "severity": "critical"}}
		hc := reconcileOnce("alertname=HighErrorRate")
		Expect(hc.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatusPending))
		Expect(hc.Status.Observations).To(HaveLen(1))
		Expect(hc.Status.Observations[0].Healthy).To(BeNil())
		Expect(hc.Status.Observations[0].Message).To(Equal("severity=critical"))
	})

	It("caps the list at the observation limit and keeps the full count in the status message", func() {
		for i := 0; i < obs.Max+4; i++ {
			querier.series = append(querier.series, map[string]string{"alertname": "Flaky", "alertstate": "firing", "pod": fmt.Sprintf("p-%02d", i)})
		}
		hc := reconcileOnce("alertname=Flaky")
		Expect(hc.Status.Observations).To(HaveLen(obs.Max))
		Expect(*hc.Status.Message).To(ContainSubstring(fmt.Sprintf("%d alerts", obs.Max+4)))
	})

	It("replaces the observations with none when a later query fails, and does not stamp lastErrorTime", func() {
		querier.series = []map[string]string{{"alertname": "HighErrorRate", "alertstate": "pending"}}
		hc := reconcileOnce("alertname=HighErrorRate")
		Expect(hc.Status.Observations).To(HaveLen(1))
		querier.err = fmt.Errorf("connection refused")
		hc = reconcileOnce("")
		Expect(hc.Status.Status).To(Equal(kuberikcomv1alpha1.HealthStatusPending))
		Expect(*hc.Status.Message).To(ContainSubstring("Failed to query Prometheus: connection refused"))
		Expect(hc.Status.Observations).To(BeEmpty())
		Expect(hc.Status.LastErrorTime).To(BeNil())
	})
})

type fakeSeriesQuerier struct {
	series []map[string]string
	err    error
}

func (f *fakeSeriesQuerier) QueryAlerts(_ context.Context, _, _ string, _, _ time.Time, _ *prometheusAuth) (*prometheusQueryResponse, error) {
	if f.err != nil {
		return nil, f.err
	}
	res := &prometheusQueryResponse{Status: "success"}
	for _, m := range f.series {
		res.Data.Result = append(res.Data.Result, struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
			Values [][]interface{}   `json:"values"`
		}{Metric: m, Values: [][]interface{}{{1.0, "1"}}})
	}
	return res, nil
}
