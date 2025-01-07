package io.numaproj.kafka.common;

import com.github.erosb.jsonsKema.*;

// TODO - comments
public class JsonValidator {

  public static boolean validate(String schemaString, String jsonString) {
    JsonValue schemaJson = new JsonParser(schemaString).parse();
    Schema schema = new SchemaLoader(schemaJson).load();
    Validator validator =
        Validator.create(schema, new ValidatorConfig(FormatValidationPolicy.ALWAYS));
    JsonValue instance = new JsonParser(jsonString).parse();
    ValidationFailure failure = validator.validate(instance);
    return failure == null;
  }
}
