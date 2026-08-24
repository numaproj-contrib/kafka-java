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

  /**
   * Copy a failure chain, replacing every message with the name of the failing class. A deserializer
   * or an Avro conversion embeds the offending field values in its message, so only the classes and
   * the stack traces of a record failure are safe to log.
   *
   * @param failure - the failure to copy
   * @return a copy carrying the class name, stack trace and sanitized cause of every link
   */
  public static Throwable sanitizeFailure(Throwable failure) {
    Throwable cause = failure.getCause();
    Throwable sanitized =
        new Throwable(failure.getClass().getName(), cause == null ? null : sanitizeFailure(cause));
    sanitized.setStackTrace(failure.getStackTrace());
    return sanitized;
  }
}
