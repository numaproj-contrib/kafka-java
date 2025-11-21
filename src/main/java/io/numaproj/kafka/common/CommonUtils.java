package io.numaproj.kafka.common;

/** Class holding common utility functions */
public class CommonUtils {

  private static final String KAFKA_KEY_PREFIX = "KAFKA_KEY:";

  /**
   * Generate a key for maps holding topic partition offsets
   *
   * @param topic - topic name
   * @param partition - partition number
   * @return a String representing a key used in topic partition maps
   */
  public static String getTopicPartitionKey(String topic, int partition) {
    return topic + ":" + partition;
  }

  /**
   * Extract Kafka message key from datum keys. If a key prefixed with "KAFKA_KEY:" is found,
   * returns the remaining string after the prefix. Otherwise returns null.
   *
   * @param keys - array of keys from the datum
   * @return the Kafka key if found, null otherwise
   */
  public static String extractKafkaKey(String[] keys) {
    if (keys == null || keys.length == 0) {
      return null;
    }
    for (String key : keys) {
      if (key != null && key.startsWith(KAFKA_KEY_PREFIX)) {
        return key.substring(KAFKA_KEY_PREFIX.length());
      }
    }
    return null;
  }
}
