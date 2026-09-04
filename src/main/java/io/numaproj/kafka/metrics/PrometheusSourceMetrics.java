package io.numaproj.kafka.metrics;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

/**
 * {@link SourceMetrics} backed by the Prometheus Java client, and the only place {@code
 * io.prometheus} is imported. Metric names carry the {@code kafka_java_} prefix.
 */
public class PrometheusSourceMetrics implements SourceMetrics {

  private final Counter skippedMessagesTotal;

  /**
   * Singleton, because the Prometheus client throws on registering the same metric name twice in
   * one registry.
   */
  private static final class DefaultHolder {
    private static final PrometheusSourceMetrics INSTANCE =
        new PrometheusSourceMetrics(PrometheusRegistry.defaultRegistry);
  }

  /** Returns the singleton instance backed by {@code PrometheusRegistry.defaultRegistry}. */
  public static PrometheusSourceMetrics defaultRegistryInstance() {
    return DefaultHolder.INSTANCE;
  }

  /** Visible so tests can use an isolated registry; production code uses the singleton. */
  PrometheusSourceMetrics(PrometheusRegistry registry) {
    this.skippedMessagesTotal =
        Counter.builder()
            .name("kafka_java_source_skipped_messages_total")
            .help("Messages the source dropped instead of forwarding them downstream.")
            .register(registry);
  }

  @Override
  public void recordSkipped() {
    skippedMessagesTotal.inc();
  }
}
