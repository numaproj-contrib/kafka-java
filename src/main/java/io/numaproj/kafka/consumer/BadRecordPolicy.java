package io.numaproj.kafka.consumer;

import io.numaproj.kafka.config.OnError;
import io.numaproj.kafka.metrics.SourceMetrics;
import io.numaproj.kafka.metrics.SourceMetrics.Action;
import java.util.function.Supplier;

/**
 * The single place the {@code onError} policy is applied to a read failure: the failure is
 * classified, the metric is incremented, and - if skipping - the drop is dispatched to the {@link
 * BadRecordSink}.
 */
final class BadRecordPolicy {

  private final OnError onError;
  private final SourceMetrics metrics;
  private final BadRecordSink sink;

  BadRecordPolicy(OnError onError, SourceMetrics metrics, BadRecordSink sink) {
    this.onError = onError;
    this.metrics = metrics;
    this.sink = sink;
  }

  /**
   * @param where the record's coordinates, for logging and the (future) dead-letter sink
   * @param stage where in the read path the failure was detected
   * @param failure the failure to classify; classification reads the cause chain, so pass the
   *     innermost cause available (e.g. {@code RecordDeserializationException.getCause()}, not the
   *     wrapper itself)
   * @param rawValue lazily supplies the record's bytes for a future dead-letter sink; never
   *     evaluated by the current (logging-only) sink
   * @return {@code true} if the record should be dropped; {@code false} if the caller must
   *     propagate the failure
   */
  boolean shouldSkip(RecordLocation where, Stage stage, Throwable failure, Supplier<byte[]> rawValue) {
    ReadErrorReason reason = ReadErrorReason.of(failure);
    if (onError != OnError.SKIP) {
      metrics.recordReadError(stage, reason, Action.FAILED);
      return false;
    }
    // TODO - follow-up PR: environmental failures (key-management or schema-registry outage,
    //  expired credentials, throttling) are skipped here like bad data. They should instead be
    //  retried or pause the vertex, because skipping them discards good records. Until then,
    //  reason=unknown on kafka_java_source_read_errors_total is the signal to alert on.
    metrics.recordReadError(stage, reason, Action.SKIPPED);
    sink.quarantine(new BadRecord(where, stage, reason, failure, rawValue));
    return true;
  }
}
