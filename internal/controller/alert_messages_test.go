package controller

import (
	"encoding/json"
	"testing"
)

func TestEvaluateAlertResultMessagesAreReadable(t *testing.T) {
	labels := map[string]string{"app": "checkout-api", "alertname": "HighErrorRate"}
	r := &PrometheusAlertHealthCheckReconciler{}
	cases := map[string]struct {
		state, want string
		empty       bool
	}{
		"none":    {empty: true, want: "No alerts firing for alertname=HighErrorRate, app=checkout-api"},
		"firing":  {state: alertStateFiring, want: "Alert firing for alertname=HighErrorRate, app=checkout-api"},
		"pending": {state: "pending", want: "Alert pending for alertname=HighErrorRate, app=checkout-api"},
	}
	for name, c := range cases {
		body := `{"data":{"result":[]}}`
		if !c.empty {
			body = `{"data":{"result":[{"metric":{"alertstate":"` + c.state + `"},"value":[0,"1"]}]}}`
		}
		res := &prometheusQueryResponse{}
		if err := json.Unmarshal([]byte(body), res); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 20; i++ { // map order is random; output must not be
			if _, msg := r.evaluateAlertResult(res, labels); msg != c.want {
				t.Fatalf("%s: got %q want %q", name, msg, c.want)
			}
		}
	}
}
