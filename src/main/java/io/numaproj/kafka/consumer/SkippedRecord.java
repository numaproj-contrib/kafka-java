package io.numaproj.kafka.consumer;

/** A record dropped by {@link SkippedRecordHandler}. */
record SkippedRecord(RecordLocation location, Throwable failure) {}
