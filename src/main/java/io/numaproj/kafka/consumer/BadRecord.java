package io.numaproj.kafka.consumer;

import io.numaproj.kafka.common.ReadErrorReason;
import io.numaproj.kafka.common.Stage;

/** A record dropped by {@link BadRecordPolicy}. */
record BadRecord(RecordLocation location, Stage stage, ReadErrorReason reason, Throwable failure) {}
