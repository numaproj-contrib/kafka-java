package io.numaproj.kafka.format;

import com.github.erosb.jsonsKema.*;
import java.io.ByteArrayInputStream;

/** jsonsKema-backed {@link JsonSchemaValidator}. Compiles the schema once at construction time. */
public class JsonsKemaSchemaValidator implements JsonSchemaValidator {

  private final Schema schema;

  public JsonsKemaSchemaValidator(String jsonSchema) {
    if (jsonSchema == null || jsonSchema.isEmpty()) {
      throw new IllegalArgumentException("JSON schema must not be null or empty");
    }
    this.schema = new SchemaLoader(new JsonParser(jsonSchema).parse()).load();
  }

  @Override
  public boolean validate(byte[] data) {
    Validator validator =
        Validator.create(schema, new ValidatorConfig(FormatValidationPolicy.ALWAYS));
    JsonValue dataJson = new JsonParser(new ByteArrayInputStream(data)).parse();
    return validator.validate(dataJson) == null;
  }
}
