package io.numaproj.kafka.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
