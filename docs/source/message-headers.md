# Message headers set by the source

### Introduction

For every record it reads, the Kafka source copies the record's own Kafka headers onto the Numaflow
message, and additionally sets `X-NF-Kafka-TopicName`. Numaflow preserves message headers across
vertices, so any downstream user-defined vertex or sink can read them.

These headers are set for every `schemaType` (`avro`, `json`, `raw`), whether or not envelope
decryption is enabled. No configuration is required.

### `X-NF-Kafka-TopicName`

The name of the Kafka topic the record was read from.

The topic is otherwise unreachable downstream. It is captured in the Numaflow `Offset` value (as
`topic:offset`), but offsets are used for ack bookkeeping and are not forwarded to sinks. The header
is what lets a vertex tell the topics apart when several Kafka source vertices fan into it, where the
topic is no longer implied by the pipeline shape.

`X-NF-Kafka-TopicName` is the same header key that Numaflow's built-in Kafka source uses, so a
downstream vertex reads the topic the same way regardless of which source fed it.

#### Reading it downstream

A user-defined sink reads it from `datum.getHeaders()`:

```java
public class MySinker extends Sinker {
  @Override
  public ResponseList processMessages(DatumIterator datumIterator) throws InterruptedException {
    ResponseList.ResponseListBuilder responses = ResponseList.newBuilder();
    Datum datum;
    // A null datum means the iterator is closed.
    while ((datum = datumIterator.next()) != null) {
      String topic = datum.getHeaders().get("X-NF-Kafka-TopicName");
      // ... route or annotate based on the source topic
      responses.addResponse(Response.responseOK(datum.getId()));
    }
    return responses.build();
  }
}
```

A user-defined map or reduce vertex reads it the same way, from `datum.getHeaders()`.

#### Precedence over a producer-supplied header

The source sets this header **after** copying the record's own headers, so if a producer wrote a
header with the same key, the actual topic the record was read from wins. All other record headers are
left untouched.
