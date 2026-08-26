package io.numaproj.kafka.metrics;

import static org.junit.jupiter.api.Assertions.*;

import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
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
  void recordSkipped_incrementsTheCounterForItsTopic() throws IOException {
    metrics.recordSkipped("orders");
    metrics.recordSkipped("orders");

    String scraped = scrape();

    assertTrue(scraped.contains("kafka_java_source_skipped_messages_total{topic=\"orders\"} 2.0"));
  }

  @Test
  void recordSkipped_countsEachTopicSeparately() throws IOException {
    metrics.recordSkipped("orders");
    metrics.recordSkipped("payments");
    metrics.recordSkipped("payments");

    String scraped = scrape();

    assertTrue(scraped.contains("kafka_java_source_skipped_messages_total{topic=\"orders\"} 1.0"));
    assertTrue(scraped.contains("kafka_java_source_skipped_messages_total{topic=\"payments\"} 2.0"));
  }

  @Test
  void recordSkipped_countsEveryDropUnderTheOneTopicLabel() throws IOException {
    metrics.recordSkipped("orders");

    String scraped = scrape();

    assertFalse(scraped.contains("stage="));
    assertFalse(scraped.contains("reason="));
  }

  @Test
  void registerTopics_thenEachTopicReportsZeroBeforeAnyDrop() throws IOException {
    // Without a series at zero, the first drop is a lone sample and rate() has nothing to subtract.
    metrics.registerTopics(List.of("orders", "payments"));

    String scraped = scrape();

    assertTrue(scraped.contains("kafka_java_source_skipped_messages_total{topic=\"orders\"} 0.0"));
    assertTrue(scraped.contains("kafka_java_source_skipped_messages_total{topic=\"payments\"} 0.0"));
  }

  @Test
  void registerTopics_thenALaterDropCountsFromZero() throws IOException {
    metrics.registerTopics(List.of("orders"));

    metrics.recordSkipped("orders");

    assertTrue(scrape().contains("kafka_java_source_skipped_messages_total{topic=\"orders\"} 1.0"));
  }

  private String scrape() throws IOException {
    MetricSnapshots snapshots = registry.scrape();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    new PrometheusTextFormatWriter(true).write(out, snapshots);
    return out.toString();
  }
}
