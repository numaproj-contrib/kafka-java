# Message headers written by the sink

For every message it publishes, the Kafka sink copies the Numaflow message headers onto the Kafka
record it produces, so headers set upstream reach the destination topic. No configuration is
required.

In a Kafka to Kafka pipeline, `X-NF-Kafka-TopicName` is copied through unchanged, so it names the
topic the record was read **from**. If a consumer of the destination topic needs a different value,
overwrite the header in a user-defined vertex upstream of the sink.
