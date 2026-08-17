package io.numaproj.kafka.consumer;

import io.numaproj.kafka.common.ReadErrorReason;
import io.numaproj.kafka.common.Stage;
import java.util.function.Supplier;

/**
 * A record dropped by {@link BadRecordPolicy}.
 *
 * @param rawValue lazy so log-and-drop never materializes (or logs) the payload. Stage {@code
 *     DECODE} failures carry the still-encrypted bytes off the wire; stage {@code CONVERT} failures
 *     carry an already-decrypted value, so a future dead-letter sink must treat the two differently.
 */
record BadRecord(
    RecordLocation location,
    Stage stage,
    ReadErrorReason reason,
    Throwable failure,
    Supplier<byte[]> rawValue) {}
