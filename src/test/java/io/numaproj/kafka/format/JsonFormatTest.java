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
  void toPayload_passesThrough() throws Exception {
    byte[] payload = "{\"name\":\"alice\"}".getBytes();
    assertSame(payload, format.toPayload(payload));
  }

  @Test
  void constructor_rejectsEmptySchema() {
    assertThrows(IllegalArgumentException.class, () -> new JsonFormat(""));
  }
}
