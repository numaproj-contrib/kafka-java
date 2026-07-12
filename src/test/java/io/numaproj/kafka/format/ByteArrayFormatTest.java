package io.numaproj.kafka.format;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ByteArrayFormatTest {

  private final ByteArrayFormat format = new ByteArrayFormat();

  @Test
  void passesBytesThroughUnchanged() throws Exception {
    byte[] bytes = "hello".getBytes();
    assertSame(bytes, format.toPayload(bytes));
    assertSame(bytes, format.toRecord(bytes));
  }
}
