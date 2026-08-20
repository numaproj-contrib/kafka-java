package io.numaproj.kafka.metrics;

import static org.junit.jupiter.api.Assertions.*;

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
  void recordSkipped_incrementsTheCounter() throws IOException {
    metrics.recordSkipped();
    metrics.recordSkipped();

    String scraped = scrape();

    assertTrue(scraped.contains("kafka_java_source_skipped_messages_total 2.0"));
  }

  @Test
  void recordSkipped_countsEveryDropUnderOneUnlabelledSeries() throws IOException {
    metrics.recordSkipped();

    String scraped = scrape();

    assertFalse(scraped.contains("stage="));
    assertFalse(scraped.contains("reason="));
  }

  private String scrape() throws IOException {
    MetricSnapshots snapshots = registry.scrape();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    new PrometheusTextFormatWriter(true).write(out, snapshots);
    return out.toString();
  }
}
