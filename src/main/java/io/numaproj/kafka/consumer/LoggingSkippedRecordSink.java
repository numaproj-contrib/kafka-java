package io.numaproj.kafka.consumer;

import lombok.extern.slf4j.Slf4j;

/** Logs the drop with the record's coordinates only - never the record itself. */
@Slf4j
final class LoggingSkippedRecordSink implements SkippedRecordSink {
  @Override
  public void quarantine(SkippedRecord skippedRecord) {
    log.warn("Dropping bad record {}", skippedRecord.location(), skippedRecord.failure());
  }
}
