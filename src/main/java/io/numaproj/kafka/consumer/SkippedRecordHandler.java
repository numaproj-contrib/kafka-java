package io.numaproj.kafka.consumer;

import io.numaproj.kafka.metrics.SourceMetrics;
import lombok.extern.slf4j.Slf4j;

/**
 * Counts and logs every message the source drops instead of forwarding. Whether to drop is the
 * caller's decision, taken from {@code onError}. Only the record's coordinates are accepted, so a
 * record's value - possibly decrypted - can never end up in a log.
 */
@Slf4j
final class SkippedRecordHandler {

  private final SourceMetrics metrics;

  SkippedRecordHandler(SourceMetrics metrics) {
    this.metrics = metrics;
  }

  /**
   * Counts and logs a record dropped because it could not be read.
   *
   * @param failure the failure, with its messages already sanitized by the caller - it is logged as
   *     is, and a deserializer or Avro message embeds the record's field values
   */
  void handleSkipped(String topic, int partition, long offset, Throwable failure) {
    metrics.recordSkipped();
    log.warn("Dropping bad record {}", coordinates(topic, partition, offset), failure);
  }

  /** The one rendering of a record's coordinates, shared by every drop log and failure message. */
  static String coordinates(String topic, int partition, long offset) {
    return "topic:%s partition:%d offset:%d".formatted(topic, partition, offset);
  }

  /** Counts a Kafka tombstone, which is dropped without being an error. */
  void handleTombstone() {
    metrics.recordSkipped();
  }
}
