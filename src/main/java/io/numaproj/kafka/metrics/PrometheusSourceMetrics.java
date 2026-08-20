package io.numaproj.kafka.metrics;

import io.numaproj.kafka.common.ReadErrorReason;
import io.numaproj.kafka.common.ReadStage;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.util.Locale;

/**
 * {@link SourceMetrics} backed by the Prometheus Java client. The only class in this codebase that
 * imports {@code io.prometheus} - every other class talks to the vendor-neutral {@link
 * SourceMetrics} interface.
 *
 * <p>Metric names follow the {@code kafka_java_} prefix convention
 * (numaproj-contrib/kafka-java#37). All label values come from closed enums, mapped to lowercase
 * strings, so cardinality is bounded at compile time.
 */
public class PrometheusSourceMetrics implements SourceMetrics {

  private final Counter readErrorsTotal;
  private final Counter recordsDroppedTotal;

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
    this.readErrorsTotal =
        Counter.builder()
            .name("kafka_java_source_read_errors_total")
            .help("Read failures encountered while polling or converting a Kafka record.")
            .labelNames("stage", "reason", "action")
            .register(registry);
    this.recordsDroppedTotal =
        Counter.builder()
            .name("kafka_java_source_records_dropped_total")
            .help("Records dropped without being an error (e.g. Kafka tombstones).")
            .labelNames("reason")
            .register(registry);
  }

  @Override
  public void recordReadError(ReadStage stage, ReadErrorReason reason, Action action) {
    readErrorsTotal.labelValues(label(stage), label(reason), label(action)).inc();
  }

  @Override
  public void recordDropped(DropReason reason) {
    recordsDroppedTotal.labelValues(label(reason)).inc();
  }

  private static String label(Enum<?> value) {
    return value.name().toLowerCase(Locale.ROOT);
  }
}
