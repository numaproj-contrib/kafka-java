package io.numaproj.kafka.config;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class OnErrorTest {

  @Test
  void from_null_returnsFail() {
    assertEquals(OnError.FAIL, OnError.from(null));
  }

  @Test
  void from_blank_returnsFail() {
    assertEquals(OnError.FAIL, OnError.from("  "));
  }

  @Test
  void from_isCaseInsensitive() {
    assertEquals(OnError.FAIL, OnError.from("fail"));
    assertEquals(OnError.FAIL, OnError.from("Fail"));
    assertEquals(OnError.FAIL, OnError.from("FAIL"));
    assertEquals(OnError.SKIP, OnError.from("skip"));
    assertEquals(OnError.SKIP, OnError.from("Skip"));
    assertEquals(OnError.SKIP, OnError.from("SKIP"));
  }

  @Test
  void from_trimsWhitespace() {
    assertEquals(OnError.SKIP, OnError.from("  skip  "));
  }

  @Test
  void from_unknownValue_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> OnError.from("dead-letter"));
    assertTrue(e.getMessage().contains("dead-letter"));
  }
}
