package io.numaproj.kafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class SourceOffsetTest {

  @Test
  void encode_writesTopicPartitionAndOffset() {
    byte[] encoded = new SourceOffset("orders", 3, 100L).encode();

    assertEquals("orders:3:100", new String(encoded, StandardCharsets.UTF_8));
  }

  @Test
  void decode_roundTripsWhatEncodeWrote() {
    SourceOffset original = new SourceOffset("orders", 3, 100L);

    assertEquals(original, SourceOffset.decode(original.encode()));
  }

  @Test
  void decode_topicNameWithThePunctuationKafkaAllows_thenParsesUnchanged() {
    SourceOffset original = new SourceOffset("my.orders_v2-east", 3, 100L);

    assertEquals(original, SourceOffset.decode(original.encode()));
  }

  @Test
  void decode_twoFieldToken_throwsRatherThanMisreadingThePartition() {
    // The pre-multi-topic token was topic:offset. Reading its offset as a partition would send
    // acks to a partition that does not exist, so it must fail loudly instead.
    assertThrows(IllegalArgumentException.class, () -> decode("orders:100"));
  }

  @Test
  void decode_singleFieldToken_throws() {
    assertThrows(IllegalArgumentException.class, () -> decode("orders"));
  }

  @Test
  void decode_moreThanThreeFields_throwsWithTheRawToken() {
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> decode("orders:3:100:extra"));

    assertTrue(thrown.getMessage().contains("orders:3:100:extra"));
  }

  @Test
  void decode_nonNumericPartition_throwsWithTheRawToken() {
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> decode("orders:three:100"));

    assertTrue(thrown.getMessage().contains("orders:three:100"));
  }

  @Test
  void decode_nonNumericOffset_throwsWithTheRawToken() {
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> decode("orders:3:hundred"));

    assertTrue(thrown.getMessage().contains("orders:3:hundred"));
  }

  @Test
  void decode_emptyToken_throws() {
    assertThrows(IllegalArgumentException.class, () -> decode(""));
  }

  private static SourceOffset decode(String token) {
    return SourceOffset.decode(token.getBytes(StandardCharsets.UTF_8));
  }
}
