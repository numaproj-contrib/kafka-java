# Source metrics

### Introduction

The Kafka source exposes its own [Prometheus](https://prometheus.io/) metrics over HTTP on port `9091`; this page does not cover shipping them to any particular backend (CloudWatch, AMP, etc).

### Metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `kafka_java_source_read_errors_total` | counter | `stage` = `decode`\|`convert`, `reason` = `bad_data`\|`unknown` | A record was skipped because it failed to be read. Only emitted under [`onError: skip`](../source/on-error.md) — under `onError: fail` the failure kills the pod instead. |
| `kafka_java_source_records_dropped_total` | counter | `reason` = `null_value` | A record was dropped without being an error (currently only Kafka tombstones). |

All label values come from closed enums, so cardinality is bounded at compile time.

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
