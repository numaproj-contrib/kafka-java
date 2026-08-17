package io.numaproj.kafka.common;

/**
 * The read-path stage at which a record failure was detected. Also a metric label.
 *
 * <p>Public: referenced from {@link io.numaproj.kafka.metrics.SourceMetrics}.
 */
public enum Stage {
  /** Deserialization (and, when enabled, decryption) inside {@code consumer.poll()}. */
  DECODE,
  /** Conversion from the deserialized Kafka value to the downstream payload. */
  CONVERT
}
