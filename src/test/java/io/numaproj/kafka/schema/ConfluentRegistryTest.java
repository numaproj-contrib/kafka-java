package io.numaproj.kafka.schema;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConfluentRegistryTest {

  private final SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);

  private ConfluentRegistry underTest;

  @BeforeEach
  public void setUp() {
    underTest = new ConfluentRegistry(schemaRegistryClient);
  }

  @Test
  public void testGetAvroSchema_success() throws Exception {
    String schemaSubject = "testTopic-value";
    int schemaVersion = 1;
    int schemaId = 1;
    Schema avroRawSchema = Schema.createRecord("TestRecord", null, "TestNamespace", false);
    SchemaMetadata schemaMetadata =
        new SchemaMetadata(schemaId, schemaVersion, "{\"type\":\"record\"}");

    when(schemaRegistryClient.getSchemaMetadata(schemaSubject, schemaVersion))
        .thenReturn(schemaMetadata);
    when(schemaRegistryClient.getSchemaById(schemaId)).thenReturn(new AvroSchema(avroRawSchema));

    Schema retrievedSchema = underTest.getAvroSchema(schemaSubject, schemaVersion);
    assertEquals(avroRawSchema, retrievedSchema);
    verify(schemaRegistryClient, times(1)).getSchemaMetadata(schemaSubject, schemaVersion);
    verify(schemaRegistryClient, times(1)).getSchemaById(schemaId);
  }

  @Test
  public void testGetAvroSchema_metadataError() throws Exception {
    String schemaSubject = "testTopic-value";
    int schemaVersion = 1;
    when(schemaRegistryClient.getSchemaMetadata(schemaSubject, schemaVersion))
        .thenThrow(new RestClientException("error", 404, 40401));
    Schema retrievedSchema = underTest.getAvroSchema(schemaSubject, schemaVersion);
    assertNull(retrievedSchema);
    verify(schemaRegistryClient, times(1)).getSchemaMetadata(schemaSubject, schemaVersion);
    verify(schemaRegistryClient, never()).getSchemaById(anyInt());
  }

  @Test
  public void testGetAvroSchema_notAvro() throws Exception {
    String schemaSubject = "testTopic-value";
    int schemaVersion = 1;
    int schemaId = 1;
    SchemaMetadata schemaMetadata = new SchemaMetadata(schemaId, schemaVersion, "JSON", null, "{}");
    when(schemaRegistryClient.getSchemaMetadata(schemaSubject, schemaVersion))
        .thenReturn(schemaMetadata);
    Schema retrievedSchema = underTest.getAvroSchema(schemaSubject, schemaVersion);
    assertNull(retrievedSchema);
    verify(schemaRegistryClient, times(1)).getSchemaMetadata(schemaSubject, schemaVersion);
    verify(schemaRegistryClient, never()).getSchemaById(anyInt());
  }

  @Test
  public void testGetJsonSchemaString_success() throws Exception {
    String subject = "test-subject";
    int schemaVersion = 1;
    int schemaId = 1;
    String schemaString = "{\"type\":\"record\",\"name\":\"Test\"}";
    SchemaMetadata schemaMetadata =
        new SchemaMetadata(schemaId, schemaVersion, "JSON", null, schemaString);
    when(schemaRegistryClient.getSchemaMetadata(subject, schemaVersion)).thenReturn(schemaMetadata);

    String result = this.underTest.getJsonSchemaString(subject, schemaVersion);
    assertEquals(schemaString, result);
  }

  @Test
  public void testGetJsonSchemaString_metadataError() throws Exception {
    String schemaSubject = "testTopic-value";
    int schemaVersion = 1;
    when(schemaRegistryClient.getSchemaMetadata(schemaSubject, schemaVersion))
        .thenThrow(new RestClientException("error", 404, 40401));
    String retrievedSchema = underTest.getJsonSchemaString(schemaSubject, schemaVersion);
    assertEquals("", retrievedSchema);
    verify(schemaRegistryClient, times(1)).getSchemaMetadata(schemaSubject, schemaVersion);
    verify(schemaRegistryClient, never()).getSchemaById(anyInt());
  }

  @Test
  public void testGetJsonSchemaString_notJSON() throws Exception {
    String schemaSubject = "testTopic-value";
    int schemaVersion = 1;
    int schemaId = 1;
    SchemaMetadata schemaMetadata = new SchemaMetadata(schemaId, schemaVersion, "AVRO", null, "{}");
    when(schemaRegistryClient.getSchemaMetadata(schemaSubject, schemaVersion))
        .thenReturn(schemaMetadata);
    String retrievedSchema = underTest.getJsonSchemaString(schemaSubject, schemaVersion);
    assertEquals("", retrievedSchema);
    verify(schemaRegistryClient, times(1)).getSchemaMetadata(schemaSubject, schemaVersion);
    verify(schemaRegistryClient, never()).getSchemaById(anyInt());
  }

  @Test
  public void testClose() throws Exception {
    underTest.close();
    verify(schemaRegistryClient, times(1)).close();
  }
}
