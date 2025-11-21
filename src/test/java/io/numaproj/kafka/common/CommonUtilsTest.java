package io.numaproj.kafka.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class CommonUtilsTest {

  @Test
  public void testGetTopicPartitionKey() {
    String topic = "test-topic";
    int partition = 0;
    String expected = "test-topic:0";
    String actual = CommonUtils.getTopicPartitionKey(topic, partition);
    assertEquals(expected, actual);
  }

  @Test
  public void testExtractKafkaKey_withPrefix() {
    String[] keys = {"KAFKA_KEY:my-custom-key"};
    String result = CommonUtils.extractKafkaKey(keys);
    assertEquals("my-custom-key", result);
  }

  @Test
  public void testExtractKafkaKey_withPrefixAndOtherKeys() {
    String[] keys = {"other-key", "KAFKA_KEY:my-custom-key", "another-key"};
    String result = CommonUtils.extractKafkaKey(keys);
    assertEquals("my-custom-key", result);
  }

  @Test
  public void testExtractKafkaKey_withMultiplePrefixKeys_returnsFirstMatch() {
    String[] keys = {"KAFKA_KEY:first-key", "KAFKA_KEY:second-key"};
    String result = CommonUtils.extractKafkaKey(keys);
    assertEquals("first-key", result);
  }

  @Test
  public void testExtractKafkaKey_withoutPrefix() {
    String[] keys = {"other-key", "another-key"};
    String result = CommonUtils.extractKafkaKey(keys);
    assertNull(result);
  }

  @Test
  public void testExtractKafkaKey_caseSensitive() {
    String[] keys = {"kafka_key:lowercase", "KAFKA_KEY:uppercase"};
    String result = CommonUtils.extractKafkaKey(keys);
    assertEquals("uppercase", result);
  }

  @Test
  public void testExtractKafkaKey_nullKeys() {
    String result = CommonUtils.extractKafkaKey(null);
    assertNull(result);
  }

  @Test
  public void testExtractKafkaKey_emptyKeys() {
    String[] keys = {};
    String result = CommonUtils.extractKafkaKey(keys);
    assertNull(result);
  }

  @Test
  public void testExtractKafkaKey_withNullInKeys() {
    String[] keys = {null, "KAFKA_KEY:valid-key"};
    String result = CommonUtils.extractKafkaKey(keys);
    assertEquals("valid-key", result);
  }

  @Test
  public void testExtractKafkaKey_prefixOnly() {
    String[] keys = {"KAFKA_KEY:"};
    String result = CommonUtils.extractKafkaKey(keys);
    assertEquals("", result);
  }
}
