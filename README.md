# Numaflow Kafka Sourcer/Sinker

## Overview

Numaflow Kafka Sourcer/Sinker is a [Numaflow](https://numaflow.numaproj.io/) user-defined source/sink connector for
Apache Kafka. It allows you to read/write data from/to a Kafka topic using Numaflow. Integrations with Confluent Schema
Registry and AWS Glue are also supported.

## Use Cases

### Read data from Kafka

Use Case 1: Read data from Kafka with an Avro schema registered in the Confluent Schema Registry. See an
example [here](docs/source/avro/avro-source.md).

Use Case 2: Read data from Kafka with an Avro schema registered in AWS Glue Schema Registry. See an
example [here](docs/source/avro-glue/avro-glue-source.md).

Use Case 3: Read data from Kafka with no schema or JSON schema registered in the Confluent Schema Registry. See an
example [here](docs/source/non-avro/non-avro-source.md).

Use Case 7: Read data from Kafka whose values are envelope-encrypted (AWS KMS + AES-256-GCM). Decryption is
opt-in and composes with any of the above `schemaType`s. See an example [here](docs/source/encrypted/decrypting-source.md).

### Write data to Kafka

Use Case 4: Write data to Kafka with an Avro schema registered in the Confluent Schema Registry. See an
example [here](docs/sink/avro/avro-sink.md).

Use Case 5: Write data to Kafka with a JSON schema registered in the Confluent Schema Registry. See an
example [here](docs/sink/json/json-sink.md).

Use Case 6: Write data to Kafka with no schema. See an example [here](docs/sink/no-schema/no-schema-sink.md).

## Upgrading from a Spring Boot version?

If you are upgrading from a Spring Boot-based release, update the **image tag** and make the following changes to your
pipeline and config specifications:

- Replace `--spring.config.location=file:/conf/user.configuration.yaml` with `--config=/conf/user.configuration.yaml`.
- The `handler` field in your config YAML is no longer required. Remove it — the handler is inferred automatically
  from the properties path argument (`--consumer.properties.path` or `--producer.properties.path`).
- Spring Boot `LOGGING_LEVEL_*` environment variables are **no longer supported**. See the logging FAQ below for the
  new approach.

## FAQ

### How do I configure logging?

This application uses **SLF4J** with **Logback** for logging (via Lombok `@Slf4j`).

The application ships with a `logback.xml` that defaults to `INFO` level and supports runtime configuration via the `ROOT_LOG_LEVEL` environment variable.

#### How do I configure logging level?

Set the `ROOT_LOG_LEVEL` environment variable in your container spec:

```yaml
env:
  - name: ROOT_LOG_LEVEL
    value: "WARN"
```

Available levels: `TRACE`, `DEBUG`, `INFO` (default), `WARN`, `ERROR`, `OFF`

To set the log level for only this application's classes (without affecting other libraries), use `KAFKA_LOG_LEVEL`:

```yaml
env:
  - name: KAFKA_LOG_LEVEL
    value: "DEBUG"
```

#### How do I enable structured logging (JSON)?

The application ships with a `logback-json.xml` that produces structured JSON logs via the
[logstash-logback-encoder](https://github.com/logfellow/logstash-logback-encoder). To activate it,
set `JAVA_TOOL_OPTIONS` in your container spec to point Logback at the JSON config file:

```yaml
env:
  - name: JAVA_TOOL_OPTIONS
    value: "-Dlogback.configurationFile=/app/resources/logback-json.xml"
```

Omitting `JAVA_TOOL_OPTIONS` uses the default plain-text format.