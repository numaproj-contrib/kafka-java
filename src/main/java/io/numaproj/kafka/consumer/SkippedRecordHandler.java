package io.numaproj.kafka.consumer;

import io.numaproj.kafka.common.ReadErrorReason;
import io.numaproj.kafka.common.ReadStage;
import io.numaproj.kafka.metrics.SourceMetrics;
import io.numaproj.kafka.metrics.SourceMetrics.DropReason;

/**
 * The single place a dropped record is counted and dispatched to the {@link SkippedRecordSink}. The
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
   * @param stage where in the read path the failure was detected
   * @param failure the failure to classify; classification reads the cause chain, so pass the
   *     innermost cause available (e.g. {@code RecordDeserializationException.getCause()}, not the
   *     wrapper itself)
   */
  // TODO - follow-up PR: environmental failures (key-management or schema-registry outage, expired
  //  credentials, throttling) are skipped here like bad data. They should instead be retried or
  //  pause the vertex, because skipping them discards good records.
  void handleSkipped(RecordLocation where, ReadStage stage, Throwable failure) {
    ReadErrorReason reason = ReadErrorReason.of(failure);
    metrics.recordReadError(stage, reason);
    sink.quarantine(new SkippedRecord(where, stage, reason, failure));
  }

  /** Counts a Kafka tombstone, which is dropped without being an error. */
  void handleTombstone() {
    metrics.recordDropped(DropReason.NULL_VALUE);
  }
}
