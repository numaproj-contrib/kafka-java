package io.numaproj.kafka.common;

/**
 * A failure attributable to the record's own bytes, as opposed to the environment (e.g. a
 * key-management or schema-registry outage).
 */
public class BadRecordException extends RuntimeException {

  public BadRecordException(String message) {
    super(message);
  }

  public BadRecordException(String message, Throwable cause) {
    super(message, cause);
  }
}
