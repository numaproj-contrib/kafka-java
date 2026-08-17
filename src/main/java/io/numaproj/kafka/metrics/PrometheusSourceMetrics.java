package io.numaproj.kafka.metrics;

import io.numaproj.kafka.common.ReadErrorReason;
import io.numaproj.kafka.common.Stage;
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

  /** Uses {@code PrometheusRegistry.defaultRegistry}. */
  public PrometheusSourceMetrics() {
    this(PrometheusRegistry.defaultRegistry);
  }

  public PrometheusSourceMetrics(PrometheusRegistry registry) {
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
  public void recordReadError(Stage stage, ReadErrorReason reason, Action action) {
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
