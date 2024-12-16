package io.numaproj.confluent.kafka_sink.schema;

import org.apache.avro.Schema;

public interface Registry {
    // getAvroSchema returns the Avro schema for the given topic name.
    // If the schema is not found, it returns null.
    Schema getAvroSchema(String topicName);
}
