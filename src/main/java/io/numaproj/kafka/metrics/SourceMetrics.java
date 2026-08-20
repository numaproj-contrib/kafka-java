package io.numaproj.kafka.metrics;

/**
 * Source-side counters. Vendor-neutral by design: no Prometheus/OTel/cloud types appear in any
 * method signature, so this interface can be implemented by any metrics backend (or none, via
 * {@link #NOOP}) without leaking a dependency into the read path.
 */
public interface SourceMetrics {

  /** Counts a message the source dropped instead of forwarding it downstream. */
  void recordSkipped();

  /** No-op implementation, used by tests and wherever metrics are not wired in (e.g. producer). */
  SourceMetrics NOOP =
      new SourceMetrics() {
        @Override
        public void recordSkipped() {
          // no-op
        }
      };
}
