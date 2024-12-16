package io.numaproj.confluent.kafka_sink.schema;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

public class ConfluentRegistryTest {
    @Mock
    private SchemaRegistryClient schemaRegistryClient;

    private ConfluentRegistry confluentRegistry;

    @BeforeEach
    public void setUp() {
        confluentRegistry = new ConfluentRegistry(schemaRegistryClient);
    }

    @Test
    public void testGetAvroSchema_success() throws Exception {
        String topicName = "testTopic";
        int schemaId = 1;
        Schema avroRawSchema = Schema.createRecord("TestRecord", null, "TestNamespace", false);
        SchemaMetadata schemaMetadata = new SchemaMetadata(schemaId, 1, "{\"type\":\"record\"}");

        when(schemaRegistryClient.getLatestSchemaMetadata(topicName + "-value")).thenReturn(schemaMetadata);
        when(schemaRegistryClient.getSchemaById(schemaId)).thenReturn(new AvroSchema(avroRawSchema));

        Schema retrievedSchema = confluentRegistry.getAvroSchema(topicName);
        assertEquals(avroRawSchema, retrievedSchema);
        verify(schemaRegistryClient, times(1)).getLatestSchemaMetadata(topicName + "-value");
        verify(schemaRegistryClient, times(1)).getSchemaById(schemaId);
    }

    @Test
    public void testGetAvroSchema_metadataError() throws Exception {
        String topicName = "testTopic";
        when(schemaRegistryClient.getLatestSchemaMetadata(topicName + "-value")).thenThrow(new RestClientException("error", 404, 40401));
        Schema retrievedSchema = confluentRegistry.getAvroSchema(topicName);
        assertNull(retrievedSchema);
        verify(schemaRegistryClient, times(1)).getLatestSchemaMetadata(topicName + "-value");
        verify(schemaRegistryClient, never()).getSchemaById(anyInt());
    }

    @Test
    public void testGetAvroSchema_notAvro() throws Exception {
        String topicName = "testTopic";
        int schemaId = 1;
        SchemaMetadata schemaMetadata = new SchemaMetadata(schemaId, 1, "JSON", null, "{}");
        when(schemaRegistryClient.getLatestSchemaMetadata(topicName + "-value")).thenReturn(schemaMetadata);
        Exception exception = assertThrows(RuntimeException.class, () -> confluentRegistry.getAvroSchema(topicName));
        assertTrue(exception.getMessage().contains("Schema type is not AVRO for topic " + topicName + "."));
    }
}
