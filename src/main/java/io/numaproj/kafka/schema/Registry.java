package io.numaproj.kafka.schema;

import org.apache.avro.Schema;

public interface Registry {
    // getAvroSchema returns the latest Avro schema for the given topic.
    // It retrieves the subject with name {topicName}-value from the schema registry.
    // If the schema is not found, it returns null.
    Schema getAvroSchema(String topicName);
}
