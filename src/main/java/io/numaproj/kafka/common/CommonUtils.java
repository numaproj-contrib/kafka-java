package io.numaproj.kafka.common;

/** Class holding common utility functions */
public class CommonUtils {

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
}
