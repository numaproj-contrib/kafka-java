package io.numaproj.kafka.format;

import static org.junit.jupiter.api.Assertions.*;

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
  void toPayload_passesThrough() throws Exception {
    byte[] payload = "{\"name\":\"alice\"}".getBytes();
    assertSame(payload, format.toPayload(payload));
  }

  @Test
  void constructor_rejectsEmptySchema() {
    assertThrows(IllegalArgumentException.class, () -> new JsonFormat(""));
  }
}
