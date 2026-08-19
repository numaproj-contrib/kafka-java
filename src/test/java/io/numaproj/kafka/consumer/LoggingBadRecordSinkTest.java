package io.numaproj.kafka.consumer;

import static org.junit.jupiter.api.Assertions.*;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.numaproj.kafka.common.ReadErrorReason;
import io.numaproj.kafka.common.ReadStage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

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

  @Test
  void quarantine_logMessageContainsCoordinatesNotPayload() {
    Logger logger = (Logger) LoggerFactory.getLogger(LoggingBadRecordSink.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      ConsumerRecord<String, byte[]> record =
          new ConsumerRecord<>("my-topic", 2, 42L, "key", "super-secret-value".getBytes());
      BadRecord badRecord =
          new BadRecord(
              RecordLocation.of(record),
              ReadStage.DECODE,
              ReadErrorReason.BAD_DATA,
              new RuntimeException("boom"));

      new LoggingBadRecordSink().quarantine(badRecord);

      assertFalse(appender.list.isEmpty());
      String message = appender.list.get(0).getFormattedMessage();
      assertTrue(message.contains("my-topic"), "log must contain topic");
      assertTrue(message.contains("partition:2"), "log must contain partition");
      assertTrue(message.contains("offset:42"), "log must contain offset");
      assertFalse(message.contains("ConsumerRecord"), "log must not render ConsumerRecord");
      assertFalse(message.contains("super-secret-value"), "log must not contain record value");
    } finally {
      logger.detachAppender(appender);
    }
  }
}
