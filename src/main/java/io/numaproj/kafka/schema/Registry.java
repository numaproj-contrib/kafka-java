package io.numaproj.kafka.schema;

import java.io.IOException;
import org.apache.avro.Schema;

/** Registry is an interface that defines the methods to interact with a schema registry. */
// TODO - we should not need to create our Schema Registry at all.
// The Kafka consumer/producer client handles the schema registry interaction behind the scenes.
public interface Registry {
  // getAvroSchema returns the latest Avro schema for the given topic.
  // It retrieves the subject with name {topicName}-value from the schema registry.
  // If the schema is not found, it returns null.
  Schema getAvroSchema(String topicName);

  // close closes the schema registry client.
  void close() throws IOException;
}
