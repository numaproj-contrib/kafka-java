package io.numaproj.kafka.consumer;

import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * The single integration point for what happens to a record dropped by {@link BadRecordPolicy}.
 * Today the only implementation counts and logs; a future dead-letter sink plugs in here with no
 * change to the read path.
 */
interface BadRecordSink {
  void quarantine(BadRecord badRecord);
}

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
