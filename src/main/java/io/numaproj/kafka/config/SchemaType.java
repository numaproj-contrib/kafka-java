package io.numaproj.kafka.config;

import java.util.Locale;

/** The message format carried over the Kafka topic — governs serializer/deserializer selection. */
public enum SchemaType {
  AVRO,
  JSON,
  RAW;

  /**
   * Parses the {@code schemaType} configuration value, case-insensitively.
   *
   * @param value the configured value
   * @return the parsed schema type
   * @throws IllegalArgumentException if {@code value} is null, blank, or not a recognised type —
   *     rejecting a typo at startup rather than silently falling back to raw
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
