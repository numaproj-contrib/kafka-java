package io.numaproj.kafka.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.numaproj.kafka.common.aws.AwsCredentials;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;

@ExtendWith(MockitoExtension.class)
class GlueRegistryTest {

  private static final String REGISTRY = "test-registry";
  private static final String SCHEMA_NAME = "numagen-avro";
  private static final String SCHEMA_DEFINITION =
      "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";

  @Mock private GlueClient glue;
  @Mock private AwsCredentials credentials;

  private GlueRegistry underTest() {
    return new GlueRegistry(glue, credentials, REGISTRY);
  }

  @Test
  void getAvroSchema_returnsParsedDefinitionAndAddressesByRegistryNameAndVersion() {
    when(glue.getSchemaVersion(any(GetSchemaVersionRequest.class)))
        .thenReturn(
            GetSchemaVersionResponse.builder()
                .dataFormat(DataFormat.AVRO)
                .schemaDefinition(SCHEMA_DEFINITION)
                .build());

    var schema = underTest().getAvroSchema(SCHEMA_NAME, 3);

    assertNotNull(schema);
    assertEquals("User", schema.getName());

    ArgumentCaptor<GetSchemaVersionRequest> captor =
        ArgumentCaptor.forClass(GetSchemaVersionRequest.class);
    verify(glue).getSchemaVersion(captor.capture());
    assertEquals(REGISTRY, captor.getValue().schemaId().registryName());
    assertEquals(SCHEMA_NAME, captor.getValue().schemaId().schemaName());
    assertEquals(3L, captor.getValue().schemaVersionNumber().versionNumber());
  }

  @Test
  void getAvroSchema_returnsNullWhenDataFormatIsNotAvro() {
    when(glue.getSchemaVersion(any(GetSchemaVersionRequest.class)))
        .thenReturn(
            GetSchemaVersionResponse.builder()
                .dataFormat(DataFormat.JSON)
                .schemaDefinition("{}")
                .build());

    assertNull(underTest().getAvroSchema(SCHEMA_NAME, 1));
  }

  @Test
  void getAvroSchema_returnsNullWhenSchemaNotFound() {
    when(glue.getSchemaVersion(any(GetSchemaVersionRequest.class)))
        .thenThrow(EntityNotFoundException.builder().message("not found").build());

    assertNull(underTest().getAvroSchema(SCHEMA_NAME, 1));
  }

  @Test
  void getAvroSchema_returnsNullWhenStoredDefinitionIsNotValidAvro() {
    // Glue reports AVRO but hands back a definition the Avro parser rejects — a registry-content
    // mismatch. The registry returns null and the application fails startup rather than sinking.
    when(glue.getSchemaVersion(any(GetSchemaVersionRequest.class)))
        .thenReturn(
            GetSchemaVersionResponse.builder()
                .dataFormat(DataFormat.AVRO)
                .schemaDefinition("{\"type\":\"record\",\"name\":\"Broken\"}")
                .build());

    assertNull(underTest().getAvroSchema(SCHEMA_NAME, 1));
  }

  @Test
  void getAvroSchema_returnsNullWhenGlueAccessIsDenied() {
    // The IAM role lacks glue:GetSchemaVersion. Same startup outcome as a missing schema: null,
    // which fetchAvroSchema turns into a fail-fast RuntimeException.
    when(glue.getSchemaVersion(any(GetSchemaVersionRequest.class)))
        .thenThrow(
            AccessDeniedException.builder()
                .message("User is not authorized to perform: glue:GetSchemaVersion")
                .build());

    assertNull(underTest().getAvroSchema(SCHEMA_NAME, 1));
  }

  @Test
  void getAvroSchema_throwsWhenSubjectOrVersionMissing() {
    assertThrows(IllegalArgumentException.class, () -> underTest().getAvroSchema("", 1));
    assertThrows(IllegalArgumentException.class, () -> underTest().getAvroSchema(SCHEMA_NAME, 0));
    verifyNoInteractions(glue);
  }

  @Test
  void getJsonSchemaString_isUnsupported() {
    // The Glue-framed contract covers Avro only.
    assertEquals("", underTest().getJsonSchemaString(SCHEMA_NAME, 1));
    verifyNoInteractions(glue);
  }

  @Test
  void close_releasesCredentialsThenGlueClient() {
    underTest().close();

    InOrder inOrder = inOrder(credentials, glue);
    inOrder.verify(credentials).close();
    inOrder.verify(glue).close();
  }

  @Test
  void create_requiresRegion() {
    assertThrows(IllegalArgumentException.class, () -> GlueRegistry.create(null, REGISTRY, null));
    assertThrows(IllegalArgumentException.class, () -> GlueRegistry.create("  ", REGISTRY, null));
  }
}
