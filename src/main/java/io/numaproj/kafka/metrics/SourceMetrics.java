package io.numaproj.kafka.metrics;

import java.util.Collection;

/**
 * Source-side counters. No metrics-backend type appears in any method signature, so the read path
 * compiles against this interface alone and any backend can implement it.
 */
public interface SourceMetrics {

  /**
   * Counts a message the source dropped instead of forwarding it downstream.
   *
   * @param topic the topic the dropped message came from, so drops stay attributable per topic
   *     under multi-topic
   */
  void recordSkipped(String topic);

  /**
   * Registers the topics the source consumes, so each reports zero before it reports a drop.
   *
   * <p>A labelled counter has no series until its first increment, so without this the first drop
   * on a topic appears as a lone sample with no predecessor and {@code rate()} cannot measure it.
   */
  void registerTopics(Collection<String> topics);
}
