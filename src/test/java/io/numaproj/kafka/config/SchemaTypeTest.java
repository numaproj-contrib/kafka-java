package io.numaproj.kafka.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SchemaTypeTest {

  @Test
  void from_isCaseInsensitive() {
    assertEquals(SchemaType.AVRO, SchemaType.from("avro"));
    assertEquals(SchemaType.AVRO, SchemaType.from("Avro"));
    assertEquals(SchemaType.AVRO, SchemaType.from("AVRO"));
    assertEquals(SchemaType.JSON, SchemaType.from("json"));
    assertEquals(SchemaType.JSON, SchemaType.from("JSON"));
    assertEquals(SchemaType.RAW, SchemaType.from("raw"));
    assertEquals(SchemaType.RAW, SchemaType.from("RAW"));
  }

  @Test
  void from_trimsWhitespace() {
    assertEquals(SchemaType.AVRO, SchemaType.from("  avro  "));
  }

  @Test
  void from_null_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SchemaType.from(null));
    assertTrue(e.getMessage().contains("required"));
  }

  @Test
  void from_blank_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SchemaType.from("  "));
    assertTrue(e.getMessage().contains("required"));
  }

  @Test
  void from_unknownValue_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SchemaType.from("parquet"));
    assertTrue(e.getMessage().contains("parquet"));
  }
}
