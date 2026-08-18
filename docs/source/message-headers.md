# Message headers set by the source

### Introduction

For every record it reads, the Kafka source copies the record's own Kafka headers onto the Numaflow
message, and additionally sets `X-NF-Kafka-TopicName`. Numaflow preserves message headers across
vertices, so any downstream user-defined vertex or sink can read them.

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
