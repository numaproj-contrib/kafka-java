package io.numaproj.kafka.common;

/**
 * A failure attributable to the record's own bytes, as opposed to the environment (e.g. a
 * key-management or schema-registry outage).
 *
 * <p>This is the one marker the core read path relies on to classify a failure. Provider adapters
 * (AWS KMS, AWS Glue, Confluent, ...) translate their own record-attributable errors into this type
 * or a subclass of it; the classifier then only needs to ask whether one appears anywhere in a
 * failure's cause chain. This keeps cloud- and vendor-specific exception taxonomies out of the
 * cloud-agnostic consumer package.
 */
public class BadRecordException extends RuntimeException {

  public BadRecordException(String message) {
    super(message);
  }

  public BadRecordException(String message, Throwable cause) {
    super(message, cause);
  }
}
