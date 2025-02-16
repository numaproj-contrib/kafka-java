package io.numaproj.kafka.common;

import com.github.erosb.jsonsKema.*;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

// JsonValidator validates raw data in byte array format against a JSON schema.
public class JsonValidator {

  // TODO - throw validation exception with more details
  public static boolean validate(String schemaString, byte[] data) {
    JsonValue schemaJson = new JsonParser(schemaString).parse();
    Schema schema = new SchemaLoader(schemaJson).load();
    Validator validator =
        Validator.create(schema, new ValidatorConfig(FormatValidationPolicy.ALWAYS));
    InputStream is = new ByteArrayInputStream(data);
    JsonValue dataJson = new JsonParser(is).parse();
    ValidationFailure failure = validator.validate(dataJson);
    return failure == null;
  }
}
