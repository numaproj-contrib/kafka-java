package io.numaproj.kafka.consumer;

/**
 * Receives a record dropped by {@link SkippedRecordHandler} and decides what becomes of it, such as
 * logging it or writing it to a dead-letter topic.
 */
interface SkippedRecordSink {
  void quarantine(SkippedRecord skippedRecord);
}
