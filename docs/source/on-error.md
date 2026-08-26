# `onError`: what the source does with a record it cannot read

### Introduction

`onError` is a `user.configuration` key that sets the source's response to any record it fails to
read, whatever the cause — a malformed schema-registry frame, an undecodable Avro or JSON payload, a
decryption failure, an unreadable envelope. It is **source-only**: a sink deployment that sets it is
unaffected.

| Value | Behavior |
|---|---|
| `fail` (default) | The failure propagates, the vertex logs it and exits, and the pod restarts. |
| `skip` | The record is dropped, counted, and logged at `WARN`; the source continues with the next record. |

```yaml
  user.configuration: |
    topicName: my-topic
    schemaType: avro
    onError: skip
```

An unrecognized value is rejected at startup rather than silently treated as `fail`.

### `onError: skip`

The `WARN` log identifies the dropped record by `topic`, `partition` and `offset` only — never the
record itself, so a decrypted or otherwise sensitive payload cannot leak into the logs. Every drop
increments `kafka_java_source_skipped_messages_total`, labelled by `topic` so drops stay
attributable when the source reads [several topics](multi-topic/multi-topic-source.md); see
[source metrics](../metrics/source-metrics.md).

Two limitations to be aware of before enabling it:

- **The record is lost.** There is no dead-letter queue yet.
- **All read failures are skipped, not just malformed records.** A KMS throttle, an expired
  credential or a schema-registry outage is dropped the same way, which discards records that are
  themselves fine. The counter alone won't tell the two apart — alert on its rate, then read the
  `WARN` logs to see which failure is driving it.

Failures outside the read path — an invalid configuration at startup, a Kafka authentication or
authorization failure — always fail fast regardless of `onError`.
