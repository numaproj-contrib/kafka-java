package io.numaproj.kafka.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class JsonValidatorTest {

  @Test
  public void testValidateJsonSchema_fail() {
    String schema =
        """
        {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "age": {
                    "type": "number",
                    "minimum": 0
                },
                "name": {
                    "type": "string"
                },
                "email": {
                    "type": "string",
                    "format": "email"
                }
            }
        }
        """;
    String data =
        """
        {
            "age": -5,
            "name": null,
            "email": "invalid"
        }
        """;
    assertFalse(JsonValidator.validate(schema, data));
  }

  @Test
  public void testValidateJsonSchema_succeed_202012() {
    String schema =
        """
        {
          "$id": "http://example.com/myURI.schema.json",
          "$schema": "https://json-schema.org/draft/2020-12/schema",
          "additionalProperties": false,
          "description": "schema for the topic numagen-json",
          "properties": {
            "Createdts": {
              "type": "integer",
              "format": "int64"
            },
            "Data": {
              "type": "object",
              "additionalProperties": false,
              "properties": {
                "value": {
                  "type": "integer",
                  "format": "int64"
                }
              }
            }
          },
          "required": [
            "Data",
            "Createdts"
          ],
          "title": "numagen-json",
          "type": "object"
        }
        """;
    String data =
        """
        {
          "Data": {
            "value": 1736093588709026645
          },
          "Createdts": 1736093588709026645
        }
        """;
    assertTrue(JsonValidator.validate(schema, data));
  }

  @Test
  public void testValidateJsonSchema_succeed_draft07() {
    String schema =
        """
    {"$schema":"http://json-schema.org/draft-07/schema#","$id":"http://example.com/myURI.schema.json","title":"numagen-json","description":"schema for the topic numagen-json","type":"object","additionalProperties":false,"required":["Data","Createdts"],"properties":{"Data":{"type":"object","properties":{"value":{"type":"integer","format":"int64"}},"additionalProperties":false},"Createdts":{"type":"integer","format":"int64"}}}
    """;
    String data =
        """
    {"Data":{"value":1736093588709026645},"Createdts":1736093588709026645}
    """;
    assertTrue(JsonValidator.validate(schema, data));
  }
}
