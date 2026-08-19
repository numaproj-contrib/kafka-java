package io.numaproj.kafka.consumer;

import io.numaproj.kafka.common.ReadErrorReason;
import io.numaproj.kafka.common.ReadStage;

/** A record dropped by {@link BadRecordPolicy}. */
record BadRecord(RecordLocation location, ReadStage stage, ReadErrorReason reason, Throwable failure) {}
