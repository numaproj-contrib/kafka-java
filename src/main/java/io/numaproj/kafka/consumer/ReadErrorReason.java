package io.numaproj.kafka.consumer;

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
  UNKNOWN
}
