# Message headers written by the sink

### Introduction

For every message it publishes, the Kafka sink copies the Numaflow message headers onto the Kafka
record it produces. Numaflow preserves message headers across vertices, so headers set by a source or
added by a user-defined vertex reach the destination topic.

Headers are copied for every `schemaType` (`avro`, `json`, `raw`), whether or not envelope encryption
is enabled. No configuration is required.

Nothing is filtered or rewritten: every header on the incoming message is written to the record as it
arrived.

### Kafka to Kafka pipelines

The Kafka source sets `X-NF-Kafka-TopicName` to the topic a record was read from. In a Kafka to Kafka
pipeline the sink copies that header through unchanged, so the record on the destination topic names
the topic it came **from**, not the one it was written to. The value does not stay stale: the next
Kafka source to read the record overwrites the header with the topic it read it from.

If a consumer of the destination topic needs the destination topic name in the header instead,
overwrite the header in a user-defined vertex upstream of the sink.

### Current limitations

* Message keys are not written as headers. A key is mapped onto the Kafka record key through the
  `KAFKA_KEY:` prefix — see the custom message keys section of the sink docs.
