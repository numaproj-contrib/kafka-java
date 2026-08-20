package io.numaproj.kafka.consumer;

/**
 * The single integration point for what happens to a record dropped by {@link
 * SkippedRecordHandler}. Today the only implementation counts and logs; a future dead-letter sink
 * plugs in here with no change to the read path.
 */
interface SkippedRecordSink {
  void quarantine(SkippedRecord skippedRecord);
}
