package io.numaproj.kafka.schema;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

public class ConfluentRegistryTest {

    private final SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);

    private ConfluentRegistry underTest;

    @BeforeEach
    public void setUp() {
        underTest = new ConfluentRegistry(schemaRegistryClient);
    }

    @Test
    public void testGetAvroSchema_success() throws Exception {
        String topicName = "testTopic";
        int schemaId = 1;
        Schema avroRawSchema = Schema.createRecord("TestRecord", null, "TestNamespace", false);
        SchemaMetadata schemaMetadata = new SchemaMetadata(schemaId, 1, "{\"type\":\"record\"}");

        when(schemaRegistryClient.getLatestSchemaMetadata(topicName + "-value")).thenReturn(schemaMetadata);
        when(schemaRegistryClient.getSchemaById(schemaId)).thenReturn(new AvroSchema(avroRawSchema));

        Schema retrievedSchema = underTest.getAvroSchema(topicName);
        assertEquals(avroRawSchema, retrievedSchema);
        verify(schemaRegistryClient, times(1)).getLatestSchemaMetadata(topicName + "-value");
        verify(schemaRegistryClient, times(1)).getSchemaById(schemaId);
    }

    @Test
    public void testGetAvroSchema_metadataError() throws Exception {
        String topicName = "testTopic";
        when(schemaRegistryClient.getLatestSchemaMetadata(topicName + "-value")).thenThrow(new RestClientException("error", 404, 40401));
        Schema retrievedSchema = underTest.getAvroSchema(topicName);
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
        Exception exception = Assertions.assertThrows(RuntimeException.class, () -> underTest.getAvroSchema(topicName));
        System.out.println("keran" + exception.getMessage());
        assertTrue(exception.getMessage().contains("Schema type is not AVRO for topic " + topicName));
    }
}
