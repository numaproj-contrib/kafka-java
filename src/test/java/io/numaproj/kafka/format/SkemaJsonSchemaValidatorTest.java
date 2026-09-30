package io.numaproj.kafka.format;

import static org.junit.jupiter.api.Assertions.*;

import com.github.erosb.jsonsKema.JsonParseException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class SkemaJsonSchemaValidatorTest {

  private static final String SCHEMA =
      "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"]}";

  private final SkemaJsonSchemaValidator validator = new SkemaJsonSchemaValidator(SCHEMA);

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
    assertThrows(IllegalArgumentException.class, () -> new SkemaJsonSchemaValidator(""));
  }

  @Test
  void constructor_rejectsMalformedSchema() {
    assertThrows(JsonParseException.class, () -> new SkemaJsonSchemaValidator("{not valid json"));
  }
}
