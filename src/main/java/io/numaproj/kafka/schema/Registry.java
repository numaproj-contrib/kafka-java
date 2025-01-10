package io.numaproj.kafka.schema;

import java.io.IOException;
import org.apache.avro.Schema;

/** Registry is an interface that defines the methods to interact with a schema registry. */
public interface Registry {
  /**
   * getAvroSchema returns the latest Avro schema for the given topic. It retrieves the subject with
   * name {topicName}-value from the schema registry. If the schema is not found, it returns null.
   */
  // TODO - throw an exception with more details
  Schema getAvroSchema(String topicName);

  /**
   * getJsonSchemaString returns the latest JSON schema string for the given topic. It retrieves the
   * subject with name {topicName}-value from the schema registry. If the schema is not found, it
   * returns null.
   */
  // TODO - throw an exception with more details
  String getJsonSchemaString(String topicName);

  // close closes the schema registry client.
  void close() throws IOException;
}
