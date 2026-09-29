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

### Partition IDs

Numaflow identifies every source partition by a single integer ID used for watermark tracking. Since
every topic numbers its partitions from 0, the source maps each `(topic, partition)` to a globally
unique ID using a fixed stride of 256:

```
globalId = topicIndex * 256 + partition
```

`topicIndex` is the topic's position in the configured list **sorted alphabetically**, so the mapping
is deterministic across every pod and restart. A single topic maps to the raw partition number, so
single-topic behavior is unchanged. New partitions added to a topic at runtime get a pre-reserved,
stable ID and are consumed automatically.

This caps each topic at **255 partitions** and a deployment at **256 topics**; the source **fails
fast at startup** if either is exceeded.
