package io.numaproj.kafka.format;

import com.github.erosb.jsonsKema.FormatValidationPolicy;
import com.github.erosb.jsonsKema.JsonParser;
import com.github.erosb.jsonsKema.JsonValue;
import com.github.erosb.jsonsKema.Schema;
import com.github.erosb.jsonsKema.SchemaLoader;
import com.github.erosb.jsonsKema.ValidationFailure;
import com.github.erosb.jsonsKema.Validator;
import com.github.erosb.jsonsKema.ValidatorConfig;
import io.numaproj.kafka.common.CommonUtils;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
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

  private final Schema schema;

  public JsonFormat(String jsonSchema) {
    if (jsonSchema == null || jsonSchema.isEmpty()) {
      throw new IllegalArgumentException("JSON schema must not be null or empty");
    }
    try {
      this.schema = new SchemaLoader(new JsonParser(jsonSchema).parse()).load();
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse or load JSON schema", e);
    }
  }

  @Override
  public byte[] toPayload(byte[] value) {
    return value;
  }

  @Override
  public byte[] toRecord(byte[] payload) throws FormatException {
    // Parsing and validation are separate steps. JsonParser.parse() throws an unchecked
    // JsonParseException for anything unparseable — a truncated or non-JSON payload, an empty one
    // (a JSON text is one value, and zero bytes is none), or a null one (an NPE when the stream
    // is built). Those are not FormatException, so they would bypass the sinker's per-message
    // catch and shut the vertex down. Convert them here, the same way AvroFormat does: one failed
    // message, batch intact.
    JsonValue dataJson;
    try {
      InputStream is = new ByteArrayInputStream(payload);
      dataJson = new JsonParser(is).parse();
    } catch (Exception e) {
      throw new FormatException(
          "Failed to parse the message as JSON", CommonUtils.sanitizeFailure(e));
    }
    // A fresh Validator per call: DefaultValidator keeps mutable per-run state.
    Validator validator =
        Validator.create(schema, new ValidatorConfig(FormatValidationPolicy.ALWAYS));
    ValidationFailure failure = validator.validate(dataJson);
    if (failure != null) {
      throw new FormatException("Failed to validate the message against the JSON schema");
    }
    return payload;
  }
}
