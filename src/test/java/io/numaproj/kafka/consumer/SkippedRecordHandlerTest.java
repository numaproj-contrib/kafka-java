package io.numaproj.kafka.consumer;

import static org.mockito.Mockito.*;

import io.numaproj.kafka.common.BadRecordException;
import io.numaproj.kafka.common.ReadErrorReason;
import io.numaproj.kafka.common.ReadStage;
import io.numaproj.kafka.metrics.SourceMetrics;
import io.numaproj.kafka.metrics.SourceMetrics.DropReason;
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
  void handleSkipped_countsAndReachesSinkWithCorrectStageAndReason() {
    RuntimeException failure = new RuntimeException("environmental");

    underTest.handleSkipped(LOCATION, ReadStage.CONVERT, failure);

    verify(metrics).recordReadError(ReadStage.CONVERT, ReadErrorReason.UNKNOWN);
    verify(sink)
        .quarantine(
            argThat(
                skippedRecord ->
                    skippedRecord.location().equals(LOCATION)
                        && skippedRecord.stage() == ReadStage.CONVERT
                        && skippedRecord.reason() == ReadErrorReason.UNKNOWN
                        && skippedRecord.failure() == failure));
  }

  @Test
  void handleSkipped_withBadRecordException_classifiesAsBadData() {
    BadRecordException badData = new BadRecordException("malformed avro body");

    underTest.handleSkipped(LOCATION, ReadStage.DECODE, badData);

    verify(metrics).recordReadError(ReadStage.DECODE, ReadErrorReason.BAD_DATA);
  }

  @Test
  void handleTombstone_countsWithoutReachingSink() {
    underTest.handleTombstone();

    verify(metrics).recordDropped(DropReason.NULL_VALUE);
    verifyNoInteractions(sink);
  }
}
