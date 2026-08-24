package io.numaproj.kafka.consumer;

import static org.mockito.Mockito.*;

import io.numaproj.kafka.metrics.SourceMetrics;
import org.junit.jupiter.api.Test;

class SkippedRecordHandlerTest {

  private final SourceMetrics metrics = mock(SourceMetrics.class);
  private final SkippedRecordHandler underTest = new SkippedRecordHandler(metrics);

  @Test
  void handleSkipped_countsTheDrop() {
    underTest.handleSkipped("t", 0, 5L, new RuntimeException("boom"));

    verify(metrics).recordSkipped();
  }

  @Test
  void handleTombstone_countsTheDrop() {
    underTest.handleTombstone();

    verify(metrics).recordSkipped();
  }
}
