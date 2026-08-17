package io.numaproj.kafka.format;

import io.numaproj.kafka.common.BadRecordException;

/** Thrown by a {@link KafkaFormat} when a value or payload cannot be converted. */
public class FormatException extends BadRecordException {

  public FormatException(String message) {
    super(message);
  }

  public FormatException(String message, Throwable cause) {
    super(message, cause);
  }
}
