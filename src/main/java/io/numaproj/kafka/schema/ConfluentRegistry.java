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
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConfluentRegistry implements Registry {

    final private SchemaRegistryClient schemaRegistryClient;

    @Autowired
    public ConfluentRegistry(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
    }

    @Override
    public Schema getAvroSchema(String topicName) {
        try {
            // Retrieve the latest schema metadata for the {topicName}-value
            SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(topicName + "-value");
            if (!Objects.equals(schemaMetadata.getSchemaType(), "AVRO")) {
                throw new RuntimeException("Schema type is not AVRO for topic " + topicName);
            }
            AvroSchema avroSchema = (AvroSchema) schemaRegistryClient.getSchemaById(schemaMetadata.getId());
            return avroSchema.rawSchema();
        } catch (IOException | RestClientException e) {
            log.error("Failed to retrieve the latest Avro schema for topic {}. {}", topicName, e.getMessage());
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        schemaRegistryClient.close();
    }
}
