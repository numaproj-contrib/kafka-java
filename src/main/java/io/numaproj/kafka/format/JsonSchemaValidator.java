package io.numaproj.kafka.format;

/** Validates a raw message payload against a JSON schema. */
public interface JsonSchemaValidator {

  boolean validate(byte[] data);
}
