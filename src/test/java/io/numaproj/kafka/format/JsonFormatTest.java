package io.numaproj.kafka.format;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class JsonFormatTest {

  private static final String SCHEMA =
      "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"]}";

  private final JsonFormat format = new JsonFormat(SCHEMA);

  @Test
  void toRecord_validPayload_passesThrough() throws Exception {
    byte[] payload = "{\"name\":\"alice\"}".getBytes();
    assertSame(payload, format.toRecord(payload));
  }

  @Test
  void toRecord_invalidPayload_throwsFormatException() {
    assertThrows(FormatException.class, () -> format.toRecord("{\"age\":1}".getBytes()));
  }

  @Test
  void toRecord_nullPayload_throwsFormatException() {
    // Not the NullPointerException the validator raises: the sinker only catches FormatException,
    // and anything else takes the vertex down.
    assertThrows(FormatException.class, () -> format.toRecord(null));
  }

  @Test
  void toRecord_emptyPayload_throwsFormatException() {
    assertThrows(FormatException.class, () -> format.toRecord(new byte[0]));
  }

  @Test
  void toRecord_unparseablePayload_throwsFormatException() {
    // The validator raises JsonParseException for these, not a false return value.
    assertThrows(FormatException.class, () -> format.toRecord("{".getBytes()));
    assertThrows(FormatException.class, () -> format.toRecord("not json".getBytes()));
    assertThrows(FormatException.class, () -> format.toRecord("   ".getBytes()));
    assertThrows(FormatException.class, () -> format.toRecord("{\"name\":\"x\"".getBytes()));
  }

  @Test
  void toRecord_malformedPayload_causeIsSanitized() {
    FormatException e =
        assertThrows(FormatException.class, () -> format.toRecord("{".getBytes()));
    assertNotNull(e.getCause());
    assertTrue(e.getCause().getMessage().endsWith("Exception"), "cause message must be a class name");
    assertTrue(e.getCause().getStackTrace().length > 0);
  }

  @Test
  void toPayload_passesThrough() throws Exception {
    byte[] payload = "{\"name\":\"alice\"}".getBytes();
    assertSame(payload, format.toPayload(payload));
  }

  @Test
  void toRecord_draft202012Schema_validPayload_passesThrough() throws Exception {
    JsonFormat draft202012 =
        new JsonFormat(
            "{\"$id\":\"http://example.com/myURI.schema.json\","
                + "\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
                + "\"additionalProperties\":false,"
                + "\"required\":[\"Data\",\"Createdts\"],"
                + "\"properties\":{"
                + "\"Data\":{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"value\":{\"type\":\"integer\",\"format\":\"int64\"}}},"
                + "\"Createdts\":{\"type\":\"integer\",\"format\":\"int64\"}},"
                + "\"title\":\"numagen-json\",\"type\":\"object\"}");
    byte[] payload =
        "{\"Data\":{\"value\":1736093588709026645},\"Createdts\":1736093588709026645}"
            .getBytes(StandardCharsets.UTF_8);
    assertSame(payload, draft202012.toRecord(payload));
  }

  @Test
  void toRecord_draft07Schema_validPayload_passesThrough() throws Exception {
    JsonFormat draft07 =
        new JsonFormat(
            "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
                + "\"type\":\"object\","
                + "\"additionalProperties\":false,"
                + "\"required\":[\"Data\",\"Createdts\"],"
                + "\"properties\":{"
                + "\"Data\":{\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"integer\",\"format\":\"int64\"}},\"additionalProperties\":false},"
                + "\"Createdts\":{\"type\":\"integer\",\"format\":\"int64\"}}}");
    byte[] payload =
        "{\"Data\":{\"value\":1736093588709026645},\"Createdts\":1736093588709026645}".getBytes();
    assertSame(payload, draft07.toRecord(payload));
  }

  @Test
  void constructor_rejectsEmptySchema() {
    assertThrows(IllegalArgumentException.class, () -> new JsonFormat(""));
  }

  @Test
  void constructor_rejectsMalformedSchema() {
    assertThrows(IllegalArgumentException.class, () -> new JsonFormat("{not valid json"));
  }

  @Test
  void constructor_rejectsInvalidSchema() {
    // Valid JSON but invalid schema: "type" must be a string, not an integer.
    // Exercises the SchemaLoadingException path, distinct from the parse-error path above.
    assertThrows(IllegalArgumentException.class, () -> new JsonFormat("{\"type\":5}"));
  }
}
