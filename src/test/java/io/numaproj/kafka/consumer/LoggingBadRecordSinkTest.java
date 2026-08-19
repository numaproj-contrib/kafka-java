package io.numaproj.kafka.consumer;

import io.numaproj.kafka.common.ReadErrorReason;
import io.numaproj.kafka.common.ReadStage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class LoggingBadRecordSinkTest {

  @Test
  void quarantine_logsWithoutThrowing() {
    ConsumerRecord<String, byte[]> record =
        new ConsumerRecord<>("t", 0, 5L, "key", "value".getBytes());
    BadRecord badRecord =
        new BadRecord(
            RecordLocation.of(record),
            ReadStage.DECODE,
            ReadErrorReason.BAD_DATA,
            new RuntimeException("boom"));

    new LoggingBadRecordSink().quarantine(badRecord);
  }
}
