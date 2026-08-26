package io.numaproj.kafka.consumer;

import java.nio.charset.StandardCharsets;

/**
 * The Kafka coordinates the source hands Numaflow as a message offset and reads back on an ack,
 * encoded as {@code topic:partition:offset} to match Numaflow's builtin Kafka source.
 *
 * <p>Carrying the Kafka partition in the token is what keeps ack in step with read: the message's
 * Numaflow partition ID is a global ID under multi-topic and so cannot be used to rebuild the key
 * the read path tracked the record under.
 *
 * <p>Source offsets never outlive the process - they go read, tracker, ack - so the durable
 * position remains the consumer group's committed offsets and this encoding can change freely.
 */
record SourceOffset(String topic, int partition, long offset) {

  private static final String SEPARATOR = ":";

  byte[] encode() {
    return (topic + SEPARATOR + partition + SEPARATOR + offset).getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Parses a token produced by {@link #encode()}.
   *
   * @throws IllegalArgumentException if the token is not three colon-separated fields with a
   *     numeric partition and offset
   */
  static SourceOffset decode(byte[] value) {
    String token = new String(value, StandardCharsets.UTF_8);
    // Kafka topic names cannot contain a colon, so the first field is always the whole topic.
    String[] fields = token.split(SEPARATOR, 3);
    if (fields.length != 3) {
      throw new IllegalArgumentException(
          "Source offset must be topic:partition:offset, got: " + token);
    }
    try {
      return new SourceOffset(fields[0], Integer.parseInt(fields[1]), Long.parseLong(fields[2]));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Source offset has a non-numeric partition or offset: " + token, e);
    }
  }
}
