# Source metrics

### Introduction

The Kafka source exposes its own [Prometheus](https://prometheus.io/) metrics over HTTP on port `9091`; this page does not cover shipping them to any particular backend (CloudWatch, AMP, etc).

### Metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `kafka_java_source_skipped_messages_total` | counter | none | A message was dropped instead of being forwarded downstream: a record skipped under [`onError: skip`](../source/on-error.md), or a Kafka tombstone. |

One unlabelled counter, so a skip is never silent and scrape cardinality is fixed. Under `onError:
fail` an unreadable record kills the pod rather than being counted here.

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
cannot be added to it. Use a **PodMonitor** or pod annotations to scrape port `9091`.
