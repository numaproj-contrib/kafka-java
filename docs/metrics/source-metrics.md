# Source metrics

### Introduction

The Kafka source exposes its own [Prometheus](https://prometheus.io/) metrics over HTTP. This is
necessary because `numaflow-java` provides no metrics API and no Prometheus client is present on the
classpath transitively - so without this endpoint, the source would be entirely unobservable from a
metrics standpoint. Numaflow's user-defined `Container` type has a `ports` field added expressly to
support this
([numaflow#2135](https://github.com/numaproj/numaflow/pull/2135): *"Expose containerPort for user
defined containers so that it can be used for prometheus metrics scraping configuration."*).

This page is vendor-neutral: it covers scraping the endpoint, not shipping the samples to any
particular backend (CloudWatch, AMP, etc).

### Metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `kafka_java_source_read_errors_total` | counter | `stage` = `decode`\|`convert`, `reason` = `bad_data`\|`unknown`, `action` = `skipped`\|`failed` | A record failed to be read. One counter covers both `onError` policies, so a single dashboard shows skip volume *and* the failure that killed the pod. |
| `kafka_java_source_records_dropped_total` | counter | `reason` = `null_value` | A record was dropped without being an error (currently only Kafka tombstones). |

All label values come from closed enums, so cardinality is bounded at compile time.

### Recommended alert

```promql
increase(kafka_java_source_read_errors_total{reason="unknown"}[5m]) > 0
```

`reason="unknown"` means records were dropped for a reason not attributable to their own bytes - e.g.
a key-management or schema-registry outage. This is the interim signal for the follow-up work that
will retry or circuit-break on such failures instead of merely counting them; see
[the onError documentation](../source/envelope-encryption/decrypting-source.md#failure-behavior).

### Endpoint

- Path: `/metrics`
- Default port: `9091` (chosen to avoid Numaflow's reserved `2469`/`2470`/`4327` ports)
- Overridable via the `KAFKA_JAVA_METRICS_PORT` environment variable; set to `0` to disable the
  endpoint entirely.

```bash
curl -s localhost:9091/metrics | grep kafka_java_source
```

### Scraping

**A `ServiceMonitor` cannot reach this port.** The `<vertex>-headless` Service that Numaflow creates has
its port map hardcoded to `metrics` (2469) and `runtime` (2470) in the controller, so a custom port
cannot be added to it. Use a **PodMonitor** or pod annotations instead.

#### PodMonitor

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: kafka-java-source
spec:
  selector:
    matchLabels:
      numaflow.numaproj.io/component: vertex # adjust to match your vertex pods
  podMetricsEndpoints:
    - port: metrics-source # must match the named container port below
      path: /metrics
```

The `udsource.container.ports` field must name the port so the `PodMonitor` can select it by name:

```yaml
udsource:
  container:
    ports:
      - name: metrics-source
        containerPort: 9091
```

This `Container` type is shared between `Pipeline` and `MonoVertex` specs.

#### Annotation fallback

If you scrape via `prometheus.io/*` pod annotations instead of the Prometheus Operator, the
annotations live in a different place depending on the deployment shape:

- **Pipeline**: `spec.vertices[].metadata.annotations`
- **MonoVertex**: `spec.metadata.annotations`

```yaml
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "9091"
  prometheus.io/path: "/metrics"
```
