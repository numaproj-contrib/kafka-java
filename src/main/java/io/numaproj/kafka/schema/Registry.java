package io.numaproj.kafka.schema;

import java.io.IOException;
import org.apache.avro.Schema;

/** Registry is an interface that defines the methods to interact with a schema registry. */
public interface Registry {

  /**
   * getSchemaById returns the schema for the given schema ID. If the schema is not found, it
   * returns null.
   */
  Schema getAvroSchema(String subject, int version);

  /**
   * getJsonSchemaString returns the JSON schema string for the given subject and version. If the
   * schema is not found, it returns an empty string "".
   */
  String getJsonSchemaString(String subject, int version);

  // close closes the schema registry client.
  void close() throws IOException;
}
