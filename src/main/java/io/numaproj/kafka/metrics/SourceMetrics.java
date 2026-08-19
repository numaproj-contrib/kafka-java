package io.numaproj.kafka.metrics;

import io.numaproj.kafka.common.ReadErrorReason;
import io.numaproj.kafka.common.ReadStage;

/**
 * Source-side counters. Vendor-neutral by design: no Prometheus/OTel/cloud types appear in any
 * method signature, so this interface can be implemented by any metrics backend (or none, via
 * {@link #NOOP}) without leaking a dependency into the read path.
 */
public interface SourceMetrics {

  /** Whether a read failure at {@code stage} was skipped (dropped) or allowed to fail the pod. */
  enum Action {
    SKIPPED,
    FAILED
  }

  /** Why a record was dropped without being counted as an error. */
  enum DropReason {
    /** A Kafka tombstone (null value). */
    NULL_VALUE
  }

  /** Counts a read failure at the given stage, classified by reason, and by the action taken. */
  void recordReadError(ReadStage stage, ReadErrorReason reason, Action action);

  /** Counts a record dropped for a reason other than an error (e.g. a tombstone). */
  void recordDropped(DropReason reason);

  /** No-op implementation, used by tests and wherever metrics are not wired in (e.g. producer). */
  SourceMetrics NOOP =
      new SourceMetrics() {
        @Override
        public void recordReadError(ReadStage stage, ReadErrorReason reason, Action action) {
          // no-op
        }

        @Override
        public void recordDropped(DropReason reason) {
          // no-op
        }
      };
}
