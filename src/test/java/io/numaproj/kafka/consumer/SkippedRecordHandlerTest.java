package io.numaproj.kafka.consumer;

import static org.mockito.Mockito.*;

import io.numaproj.kafka.metrics.SourceMetrics;
import org.junit.jupiter.api.Test;

class SkippedRecordHandlerTest {

  private final SourceMetrics metrics = mock(SourceMetrics.class);
  private final SkippedRecordHandler underTest = new SkippedRecordHandler(metrics);

  @Test
  void handleSkipped_countsTheDropAgainstItsTopic() {
    underTest.handleSkipped("orders", 0, 5L, new RuntimeException("boom"));

    verify(metrics).recordSkipped("orders");
  }

  @Test
  void handleTombstone_countsTheDropAgainstItsTopic() {
    underTest.handleTombstone("orders");

    verify(metrics).recordSkipped("orders");
  }
}
