package io.numaproj.kafka.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.numaproj.kafka.common.BadRecordException;
import io.numaproj.kafka.common.ReadErrorReason;
import io.numaproj.kafka.common.Stage;
import io.numaproj.kafka.config.OnError;
import io.numaproj.kafka.metrics.SourceMetrics;
import io.numaproj.kafka.metrics.SourceMetrics.Action;
import org.junit.jupiter.api.Test;

class BadRecordPolicyTest {

  private static final RecordLocation LOCATION = RecordLocation.of(sampleRecord());

  private final SourceMetrics metrics = mock(SourceMetrics.class);
  private final BadRecordSink sink = mock(BadRecordSink.class);

  private static org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]> sampleRecord() {
    return new org.apache.kafka.clients.consumer.ConsumerRecord<>(
        "t", 0, 5L, "key", "value".getBytes());
  }

  @Test
  void shouldSkip_onErrorFail_neverSkipsAndCountsFailed() {
    BadRecordPolicy policy = new BadRecordPolicy(OnError.FAIL, metrics, sink);
    BadRecordException failure = new BadRecordException("bad");

    boolean skipped = policy.shouldSkip(LOCATION, Stage.DECODE, failure, () -> new byte[0]);

    assertFalse(skipped);
    verify(metrics).recordReadError(Stage.DECODE, ReadErrorReason.BAD_DATA, Action.FAILED);
    verify(sink, never()).quarantine(any());
  }

  @Test
  void shouldSkip_onErrorSkip_skipsAndReachesSinkWithCorrectStageAndReason() {
    BadRecordPolicy policy = new BadRecordPolicy(OnError.SKIP, metrics, sink);
    RuntimeException failure = new RuntimeException("environmental");

    boolean skipped = policy.shouldSkip(LOCATION, Stage.CONVERT, failure, () -> new byte[0]);

    assertTrue(skipped);
    verify(metrics).recordReadError(Stage.CONVERT, ReadErrorReason.UNKNOWN, Action.SKIPPED);
    verify(sink)
        .quarantine(
            argThat(
                badRecord ->
                    badRecord.location().equals(LOCATION)
                        && badRecord.stage() == Stage.CONVERT
                        && badRecord.reason() == ReadErrorReason.UNKNOWN
                        && badRecord.failure() == failure));
  }

  @Test
  void shouldSkip_onErrorFail_refusalNeverReachesSink() {
    BadRecordPolicy policy = new BadRecordPolicy(OnError.FAIL, metrics, sink);

    policy.shouldSkip(LOCATION, Stage.DECODE, new BadRecordException("bad"), () -> new byte[0]);

    verifyNoInteractions(sink);
  }
}
