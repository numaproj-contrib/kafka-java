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
    // The validator parses before it validates, and only the validation step is reported through the
    // return value: anything unparseable — a truncated or non-JSON payload, an empty one (a JSON
    // text is one value, and zero bytes is none), a null one (dereferenced outright) — leaves it as
    // an unchecked exception instead. Those are not FormatException, so they would pass through the
    // sinker's per-message catch and shut the vertex down. Convert them here, the same way
    // AvroFormat does around its decode: one failed message, batch intact.
    boolean valid;
    try {
      valid = JsonValidator.validate(jsonSchema, payload);
    } catch (Exception e) {
      throw new FormatException("Failed to parse the message as JSON", e);
    }
    if (!valid) {
      throw new FormatException("Failed to validate the message against the JSON schema");
    }
    return payload;
  }
}
