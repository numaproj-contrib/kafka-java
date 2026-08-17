package io.numaproj.kafka.consumer;

import lombok.extern.slf4j.Slf4j;

/** Logs the drop with the record's coordinates only - never the record itself. */
@Slf4j
final class LoggingBadRecordSink implements BadRecordSink {
  @Override
  public void quarantine(BadRecord badRecord) {
    log.warn(
        "Dropping bad record {} stage:{} reason:{}",
        badRecord.location(),
        badRecord.stage(),
        badRecord.reason(),
        badRecord.failure());
  }
}
