package io.numaproj.kafka.consumer;

import static org.junit.jupiter.api.Assertions.*;

import io.numaproj.kafka.common.ReadErrorReason;
import io.numaproj.kafka.common.Stage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class LoggingBadRecordSinkTest {

  @Test
  void quarantine_neverInvokesRawValueSupplier() {
    ConsumerRecord<String, byte[]> record =
        new ConsumerRecord<>("t", 0, 5L, "key", "value".getBytes());
    boolean[] invoked = {false};
    BadRecord badRecord =
        new BadRecord(
            RecordLocation.of(record),
            Stage.DECODE,
            ReadErrorReason.BAD_DATA,
            new RuntimeException("boom"),
            () -> {
              invoked[0] = true;
              return "super-secret-decrypted-payload".getBytes();
            });

    new LoggingBadRecordSink().quarantine(badRecord);

    assertFalse(invoked[0], "the raw payload supplier must never be evaluated by a logging sink");
  }
}
