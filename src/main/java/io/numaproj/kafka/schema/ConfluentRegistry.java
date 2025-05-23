package io.numaproj.kafka.schema;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@ConditionalOnExpression("'${schemaType}'.equals('json') or '${schemaType}'.equals('avro')")
public class ConfluentRegistry implements Registry {

  private final SchemaRegistryClient schemaRegistryClient;

  @Autowired
  public ConfluentRegistry(SchemaRegistryClient schemaRegistryClient) {
    this.schemaRegistryClient = schemaRegistryClient;
  }

  @Override
  public Schema getAvroSchema(String subject, int version) {
    try {
      if (!subject.isEmpty() && version != 0) {
        SchemaMetadata schemaMetadata = schemaRegistryClient.getSchemaMetadata(subject, version);
        if (!Objects.equals(schemaMetadata.getSchemaType(), "AVRO")) {
          log.error("Schema type is not AVRO for subject {}, version {}.", subject, version);
          return null;
        }
        AvroSchema avroSchema =
            (AvroSchema) schemaRegistryClient.getSchemaById(schemaMetadata.getId());
        return avroSchema.rawSchema();
      }
    } catch (IOException | RestClientException e) {
      log.error(
          "Failed to retrieve the Avro schema for subject {}, version {}. {}",
          subject,
          version,
          e.getMessage());
    }
    return null;
  }

  @Override
  public String getJsonSchemaString(String subject, int version) {
    try {
      SchemaMetadata schemaMetadata = schemaRegistryClient.getSchemaMetadata(subject, version);
      if (!Objects.equals(schemaMetadata.getSchemaType(), "JSON")) {
        log.error("Schema type is not JSON for subject {}, version {}.", subject, version);
        return "";
      }
      return schemaMetadata.getSchema();
    } catch (IOException | RestClientException e) {
      log.error(
          "Failed to retrieve the JSON schema string for subject {}, version {}. {}",
          subject,
          version,
          e.getMessage());
    }
    return "";
  }

  @Override
  public void close() throws IOException {
    schemaRegistryClient.close();
  }
}
