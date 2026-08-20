package io.numaproj.kafka.metrics;

import io.numaproj.kafka.common.ReadErrorReason;
import io.numaproj.kafka.common.ReadStage;

/**
 * Source-side counters. Vendor-neutral by design: no Prometheus/OTel/cloud types appear in any
 * method signature, so this interface can be implemented by any metrics backend (or none, via
 * {@link #NOOP}) without leaking a dependency into the read path.
 */
public interface SourceMetrics {

  /** Why a record was dropped without being counted as an error. */
  enum DropReason {
    /** A Kafka tombstone (null value). */
    NULL_VALUE
  }

  /** Counts a record skipped because it failed to be read at the given stage. */
  void recordReadError(ReadStage stage, ReadErrorReason reason);

  /** Counts a record dropped for a reason other than an error (e.g. a tombstone). */
  void recordDropped(DropReason reason);

  /** No-op implementation, used by tests and wherever metrics are not wired in (e.g. producer). */
  SourceMetrics NOOP =
      new SourceMetrics() {
        @Override
        public void recordReadError(ReadStage stage, ReadErrorReason reason) {
          // no-op
        }

        @Override
        public void recordDropped(DropReason reason) {
          // no-op
        }
      };
}
