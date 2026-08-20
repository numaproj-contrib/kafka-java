package io.numaproj.kafka.consumer;

/**
 * A record that could not be deserialized, identified by its coordinates only. The cause is the
 * deserializer's own failure.
 */
final class PoisonRecordException extends RuntimeException {

  private final RecordLocation location;

  PoisonRecordException(RecordLocation location, Throwable cause) {
    super("Failed to deserialize the record " + location, cause);
    this.location = location;
  }

  RecordLocation location() {
    return location;
  }
}
