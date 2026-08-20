package io.numaproj.kafka.consumer;

import io.numaproj.kafka.metrics.SourceMetrics;

/**
 * The single place a dropped message is counted and dispatched to the {@link SkippedRecordSink}. The
 * caller decides whether to drop (from {@code onError}); this class decides nothing.
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
   * @param where the record's coordinates, for logging and the (future) dead-letter sink
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
