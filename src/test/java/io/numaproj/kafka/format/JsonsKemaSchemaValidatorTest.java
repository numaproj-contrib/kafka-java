package io.numaproj.kafka.format;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class JsonsKemaSchemaValidatorTest {

  private static final String SCHEMA =
      "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"]}";

  private final JsonsKemaSchemaValidator validator = new JsonsKemaSchemaValidator(SCHEMA);

  @Test
  void validate_validPayload_returnsTrue() {
    assertTrue(validator.validate("{\"name\":\"alice\"}".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void validate_invalidPayload_returnsFalse() {
    assertFalse(validator.validate("{\"age\":1}".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void constructor_rejectsEmptySchema() {
    assertThrows(IllegalArgumentException.class, () -> new JsonsKemaSchemaValidator(""));
  }

  @Test
  void constructor_rejectsMalformedSchema() {
    assertThrows(Exception.class, () -> new JsonsKemaSchemaValidator("{not valid json"));
  }
}
