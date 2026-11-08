# prometheus-controller
// TODO(user): Add simple overview of use/purpose

## Description
// TODO(user): An in-depth paragraph about your project and overview of use

## Getting Started

### Prerequisites
- go version v1.24.6+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/prometheus-controller:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/prometheus-controller:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Project Distribution

Following the options to release and provide this solution to the users.

### By providing a bundle with all YAML files

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/prometheus-controller:tag
```

**NOTE:** The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without its
dependencies.

2. Using the installer

Users can just run 'kubectl apply -f <URL for YAML BUNDLE>' to install
the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/prometheus-controller/<tag or branch>/dist/install.yaml
```

### By providing a Helm Chart

1. Build the chart using the optional helm plugin

```sh
kubebuilder edit --plugins=helm/v2-alpha
```

2. See that a chart was generated under 'dist/chart', and users
can obtain this solution from there.

**NOTE:** If you change the project, you need to update the Helm Chart
using the same command above to sync the latest changes. Furthermore,
if you create webhooks, you need to use the above command with
the '--force' flag and manually ensure that any custom configuration
previously added to 'dist/chart/values.yaml' or 'dist/chart/manager/manager.yaml'
is manually re-applied afterwards.

## Observations

`prometheus-alert` HealthChecks also write one typed entry to `status.observations` (requires a rollout-controller that has the field), which the dashboard draws as a figure. Status and message are unchanged. The controller reads the alerts' evaluated values from `GET /api/v1/rules?type=alert`, filtered by the same `prometheus-alert-labels`; if that call fails, observations degrade but the status is still reported.

| Annotation | Meaning |
|---|---|
| `healthcheck.kuberik.com/observation-limit` | The alert's limit, a plain decimal (`0.05`, no exponent). Prometheus does not expose it, so declare it here. |
| `healthcheck.kuberik.com/observation-direction` | `Above` or `Below`: which side of the limit is bad. Required with the limit. |
| `healthcheck.kuberik.com/observation-unit` | Optional unit (`Percent`, `Ratio`, `Milliseconds`, ...). |
| `healthcheck.kuberik.com/observation-name` | Optional name; defaults to the `alertname` label. |

What gets written:

- **Threshold**: a limit is known and at least one matching alert is pending or firing. `value` is the worst evaluated value across matching alerts (highest for `Above`, lowest for `Below`); `healthy` is false while any is firing, true when only pending. The limit comes from the annotations, or, when they are absent, from a trivial rule expression (`<expr> > N`, `>=`, `<`, `<=`; compound conditions, `==`/`!=`, `bool` and comparisons between two series are not inferred, and matching rules must agree on limit and direction).
- **State** (name and `healthy` only): the limit is declared by annotation but no alert is active, so there is no value; `healthy` is true unless the ALERTS lookup saw a firing alert.
- **Count** named `firing alerts`: no limit is declared or inferable (or the annotations are invalid). `healthy` is true when no alert is firing; pending alerts are not counted.
- `observedTime` changes only when the observation does.

```yaml
apiVersion: kuberik.com/v1alpha1
kind: HealthCheck
metadata:
  name: error-rate
  annotations:
    healthcheck.kuberik.com/prometheus-url: http://prometheus.monitoring:9090
    healthcheck.kuberik.com/prometheus-alert-labels: alertname=HighErrorRate,severity=critical
    healthcheck.kuberik.com/observation-limit: "0.05"
    healthcheck.kuberik.com/observation-direction: Above
    healthcheck.kuberik.com/observation-name: error rate
spec:
  class: prometheus-alert
```

## Contributing
// TODO(user): Add detailed information on how you would like others to contribute to this project

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

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

