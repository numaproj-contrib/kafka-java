package io.numaproj.kafka.config;

import java.util.Locale;

/** The message format of the data carried over the Kafka topic. */
public enum SchemaType {
  AVRO,
  JSON,
  RAW;

  /**
   * Parses {@code schemaType} case-insensitively; throws on null, blank, or unrecognised values.
   */
  public static SchemaType from(String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(
          "--schemaType is required. Must be one of: avro, json, raw");
    }
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid schemaType: '" + value + "'. Must be one of: avro, json, raw", e);
    }
  }
}
