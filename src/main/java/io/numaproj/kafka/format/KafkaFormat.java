package io.numaproj.kafka.format;

/**
 * Describes how a single message format is carried over Kafka, in both directions.
 *
 * <p>A {@code KafkaFormat} is the one point of variation between the supported formats (Avro, JSON,
 * raw byte array). Everything else — polling, offset tracking, batching, acking and flushing — is
 * format agnostic and lives in the shared {@code KafkaSourcer} and {@code KafkaSinker}. To add a new
 * format, implement this interface; no changes to the sourcer or sinker are required.
 *
 * <p>The type parameter {@code V} is the value type exchanged with the Kafka client (for example
 * {@code GenericRecord} for Avro, {@code byte[]} for raw/JSON). It ties the consumer's deserializer
 * and the producer's serializer to a single, consistent type.
 *
 * @param <V> the Kafka record value type this format reads and writes
 */
public interface KafkaFormat<V> {

  /**
   * Source direction: convert a value read from Kafka into the payload forwarded to the next
   * Numaflow vertex.
   *
   * @param value the deserialized Kafka record value, never {@code null}
   * @return the payload bytes to forward downstream
   * @throws FormatException if the value cannot be converted
   */
  byte[] toPayload(V value) throws FormatException;

  /**
   * Sink direction: convert an incoming Numaflow payload into the value written to Kafka.
   *
   * @param payload the raw payload bytes received from the previous Numaflow vertex
   * @return the value to publish to Kafka
   * @throws FormatException if the payload is invalid for this format
   */
  V toRecord(byte[] payload) throws FormatException;
}
