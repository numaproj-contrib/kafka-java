# Consuming from multiple topics

### Introduction

`topicName` accepts a **comma-separated list** of topics on the **same** Kafka cluster. The source
subscribes to all of them with a single shared consumer group and merges them into one downstream
stream. A value with no commas behaves exactly as before, so single-topic deployments need no
changes.

```yaml
  user.configuration: |
    topicName: topic-a, topic-b, topic-c
    schemaType: avro
    onError: skip # optional; default: fail
```

- The value is split on commas and each name is trimmed of surrounding whitespace.
- Empty entries are dropped; a value with no real topic (e.g. `","`) is rejected at startup.
- Every outgoing message carries the source topic it came from in the `X-NF-Kafka-TopicName` header;
  see [message headers](message-headers.md).
- Bad-message handling ([`onError`](on-error.md)) applies uniformly across all configured topics.

### When to use

- Multiple **low-throughput** topics on the **same cluster** that feed the same pipeline.
- All topics share the **same** `schemaType` (all Avro, all JSON, or all raw). Different Avro schemas
  are fine as long as they share the format type and, for Glue, the same AWS region.

### Glue schema registry and envelope encryption

Multi-topic composes with both [Glue](avro-glue/avro-glue-source.md) and
[envelope encryption](envelope-encryption/decrypting-source.md): both work **per record** (Glue
resolves each record's schema from its embedded version ID; decryption unwraps each envelope with the
configured KMS key), so all topics must share the same AWS region and the **same KMS key**.

### When not to use

- Topics on **different clusters**, using **different format types**, or encrypted under
  **different KMS keys**.
- Topics with **significantly different throughput** or that **require independent scaling**.
- A stream feeding a **reduce (windowed aggregation)** vertex: all topics share a single watermark,
  so one idle or lagging topic can stall or corrupt window firing for the others.

### Current limitation

In multi-topic mode the raw Kafka partition numbers are reported deduplicated and are **not yet
normalized into globally unique IDs**. Offset commits stay correct regardless (the topic name is
carried in the offset value), so this is safe for map/flatmap pipelines. Watermark correctness for
windowing vertices depends on partition-ID normalization, which is not implemented yet — do not use
multi-topic with reduce.
