# Message headers set by the source

### Introduction

For every record it reads, the Kafka source copies the record's own Kafka headers onto the Numaflow
message, and additionally sets `X-NF-Kafka-TopicName`. Numaflow preserves message headers across
vertices, so any downstream user-defined vertex or sink can read them.

### Headers with a null value

A Kafka header value is nullable on the wire, and some producers use a null value to mark a header
whose presence is itself the signal. Numaflow message headers travel over gRPC as a protobuf
`map<string, string>`, whose values cannot be null, so the null has to become something else.

The source keeps the key and surfaces the value as an empty string. A header with a null value and
one with a zero-length value are therefore indistinguishable downstream. That is a deliberate
trade-off: the alternative is to drop the header entirely, which would lose the one thing such a
header is usually meant to convey - that the key is there. An ambiguity between null and empty costs
less than silently discarding the signal.

### `X-NF-Kafka-TopicName`

The name of the Kafka topic the record was read from.

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
