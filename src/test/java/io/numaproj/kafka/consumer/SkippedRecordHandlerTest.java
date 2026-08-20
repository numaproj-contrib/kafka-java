package io.numaproj.kafka.consumer;

import static org.mockito.Mockito.*;

import io.numaproj.kafka.metrics.SourceMetrics;
import org.junit.jupiter.api.Test;

class SkippedRecordHandlerTest {

  private static final RecordLocation LOCATION = RecordLocation.of(sampleRecord());

  private final SourceMetrics metrics = mock(SourceMetrics.class);
  private final SkippedRecordSink sink = mock(SkippedRecordSink.class);
  private final SkippedRecordHandler underTest = new SkippedRecordHandler(metrics, sink);

  private static org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]> sampleRecord() {
    return new org.apache.kafka.clients.consumer.ConsumerRecord<>(
        "t", 0, 5L, "key", "value".getBytes());
  }

  @Test
  void handleSkipped_countsAndReachesSinkWithTheFailure() {
    RuntimeException failure = new RuntimeException("boom");

    underTest.handleSkipped(LOCATION, failure);

    verify(metrics).recordSkipped();
    verify(sink)
        .quarantine(
            argThat(
                skippedRecord ->
                    skippedRecord.location().equals(LOCATION)
                        && skippedRecord.failure() == failure));
  }

  @Test
  void handleTombstone_countsWithoutReachingSink() {
    underTest.handleTombstone();

    verify(metrics).recordSkipped();
    verifyNoInteractions(sink);
  }
}
