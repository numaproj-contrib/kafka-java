package io.numaproj.kafka.consumer;

import io.numaproj.kafka.common.BadRecordException;
import io.numaproj.kafka.format.FormatException;

/**
 * The closed, low-cardinality classification of a read-path failure. Used as a metric label, not a
 * policy gate: {@code onError: skip} skips every read failure regardless of reason for now, and this
 * enum exists so the drop is at least measurable and attributable.
 *
 * <p>Public: referenced from {@link io.numaproj.kafka.metrics.SourceMetrics}.
 */
public enum ReadErrorReason {
  /** The record's own bytes are bad: bad envelope, AEAD tag failure, undecodable Avro/JSON. */
  BAD_DATA,
  /** Not positively attributable to the record - e.g. a key-management or registry outage. */
  UNKNOWN;

  /**
   * Classifies a failure by walking its cause chain for a {@link BadRecordException} or {@link
   * FormatException}.
   *
   * <p><b>Trap:</b> {@code RecordDeserializationException} extends Kafka's {@code
   * SerializationException}, not {@code BadRecordException}. Callers at the decode stage must pass
   * {@code getCause()}, never the wrapper itself, or every decode failure classifies as {@link
   * #UNKNOWN}.
   */
  static ReadErrorReason of(Throwable failure) {
    for (Throwable t = failure; t != null; t = t.getCause()) {
      if (t instanceof BadRecordException || t instanceof FormatException) {
        return BAD_DATA;
      }
    }
    return UNKNOWN;
  }
}
