package io.numaproj.kafka.metrics;

/**
 * Source-side counters. No metrics-backend type appears in any method signature, so the read path
 * compiles against this interface alone and any backend can implement it.
 */
public interface SourceMetrics {

  /** Counts a message the source dropped instead of forwarding it downstream. */
  void recordSkipped();
}
