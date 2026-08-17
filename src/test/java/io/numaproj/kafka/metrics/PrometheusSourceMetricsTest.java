package io.numaproj.kafka.metrics;

import static org.junit.jupiter.api.Assertions.*;

import io.numaproj.kafka.common.ReadErrorReason;
import io.numaproj.kafka.common.Stage;
import io.numaproj.kafka.metrics.SourceMetrics.Action;
import io.numaproj.kafka.metrics.SourceMetrics.DropReason;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PrometheusSourceMetricsTest {

  private PrometheusRegistry registry;
  private PrometheusSourceMetrics metrics;

  @BeforeEach
  void setUp() {
    registry = new PrometheusRegistry();
    metrics = new PrometheusSourceMetrics(registry);
  }

  @Test
  void recordReadError_incrementsCounterWithLowercaseLabels() throws IOException {
    metrics.recordReadError(Stage.DECODE, ReadErrorReason.BAD_DATA, Action.SKIPPED);

    String scraped = scrape();

    assertTrue(scraped.contains("kafka_java_source_read_errors_total"));
    assertTrue(scraped.contains("stage=\"decode\""));
    assertTrue(scraped.contains("reason=\"bad_data\""));
    assertTrue(scraped.contains("action=\"skipped\""));
  }

  @Test
  void recordDropped_incrementsCounterWithLowercaseLabel() throws IOException {
    metrics.recordDropped(DropReason.NULL_VALUE);

    String scraped = scrape();

    assertTrue(scraped.contains("kafka_java_source_records_dropped_total"));
    assertTrue(scraped.contains("reason=\"null_value\""));
  }

  private String scrape() throws IOException {
    MetricSnapshots snapshots = registry.scrape();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    new PrometheusTextFormatWriter(true).write(out, snapshots);
    return out.toString();
  }
}
