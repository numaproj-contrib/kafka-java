package io.numaproj.kafka.consumer;

import io.numaproj.kafka.metrics.SourceMetrics;

/**
 * Counts a dropped message and dispatches it to the {@link SkippedRecordSink}. Whether to drop is
 * the caller's decision, taken from {@code onError}.
 */
final class SkippedRecordHandler {

  private final SourceMetrics metrics;
  private final SkippedRecordSink sink;

  SkippedRecordHandler(SourceMetrics metrics, SkippedRecordSink sink) {
    this.metrics = metrics;
    this.sink = sink;
  }

  /**
   * Counts and dispatches a record dropped because it could not be read.
   *
   * @param where the record's coordinates, which identify it to the sink without exposing its value
   * @param failure why the record could not be read
   */
  void handleSkipped(RecordLocation where, Throwable failure) {
    metrics.recordSkipped();
    sink.quarantine(new SkippedRecord(where, failure));
  }

  /** Counts a Kafka tombstone, which is dropped without being an error. */
  void handleTombstone() {
    metrics.recordSkipped();
  }
}
