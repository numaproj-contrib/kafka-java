package io.numaproj.kafka.format;

import io.numaproj.kafka.common.JsonValidator;
import lombok.extern.slf4j.Slf4j;

/**
 * JSON format backed by a JSON schema.
 *
 * <p>On the sink side the raw payload is validated against the supplied JSON schema and, when valid,
 * written to Kafka unchanged (a byte-array serializer is used on the client). Validation is done
 * here rather than via the Confluent {@code KafkaJsonSchemaSerializer} because the latter requires a
 * POJO with annotations, which prevents a generic, schema-driven solution.
 *
 * <p>On the source side payloads pass through unchanged.
 */
@Slf4j
public class JsonFormat implements KafkaFormat<byte[]> {

  private final String jsonSchema;

  public JsonFormat(String jsonSchema) {
    if (jsonSchema == null || jsonSchema.isEmpty()) {
      throw new IllegalArgumentException("JSON schema must not be null or empty");
    }
    this.jsonSchema = jsonSchema;
  }

  @Override
  public byte[] toPayload(byte[] value) {
    return value;
  }

  @Override
  public byte[] toRecord(byte[] payload) throws FormatException {
    if (!JsonValidator.validate(jsonSchema, payload)) {
      throw new FormatException("Failed to validate the message against the JSON schema");
    }
    return payload;
  }
}
