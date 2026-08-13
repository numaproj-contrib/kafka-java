# Publish to a topic using AWS Glue Schema Registry

### Introduction

This document demonstrates how to publish Avro messages to a topic whose schemas live in the
[AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html) rather than
a Confluent registry. Set `schema.registry.type=glue` in `producer.properties` and the sink uses
`com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer` for the value;
the key still uses `org.apache.kafka.common.serialization.StringSerializer`.

The value is written in the Glue **binary wire format**: an 18-byte header (header version `0x03`, a
compression flag, then the 16-byte schema-version UUID) followed by the Avro body.

As with the Confluent path, the incoming payload is expected to be JSON matching the schema; the sink
decodes it into an Avro `GenericRecord` before the serializer frames it.

For the source-side equivalent, see [avro-glue-source](../../source/avro-glue/avro-glue-source.md).

### Compression

**The Avro body is compressed with zlib by default.** You do not need to configure anything, and
neither does the consumer — the frame records which compression was used.

To turn it off, set this in `producer.properties`:

```properties
compression=NONE
```

`ZLIB` (the default) and `NONE` are the only accepted values.

> If you have used the AWS Glue serializer directly: its own default is *no* compression.
> `kafka-java` defaults to `ZLIB` instead.

Leave Kafka's own `compression.type` alone — it is `none` by default, and compressing an
already-compressed (and possibly encrypted) payload buys nothing.

### Schema registration

Schemas must **already exist** in the registry. `kafka-java` never registers a schema on your behalf
— it disables auto-registration on every path, so a schema definition that is not registered fails
rather than being created implicitly. The sink reads the registered definition at startup and the
serializer resolves that same definition back to a schema-version id, so the two must match exactly.

### Configuration

| Property | Required | Default | Description |
|---|---|---|---|
| `schema.registry.type` | Yes | `confluent` | Set to `glue` to use the Glue Schema Registry |
| `region` | Yes | — | AWS region of the Glue registry (e.g. `us-east-1`) |
| `registry.name` | No | `default-registry` | Name of the Glue registry |
| `compression` | No | `ZLIB` | Compression of the Avro body; set `NONE` to turn it off |
| `assumeRoleArn` | No | — | IAM role ARN to assume before connecting to Glue (and KMS) |

Of these, `schema.registry.type` is managed by kafka-java. All other properties are passed through
directly to the Glue serializer.

In `user.configuration`:

* `topicName` — the Kafka topic to write to.
* `schemaType` — `avro`.
* `schemaSubject` — the **Glue schema name**.
* `schemaVersion` — the schema version number.

> `schemaType: json` is not supported with `schema.registry.type=glue`; the Glue-framed contract covers
> Avro only. The sink fails at startup with a clear message rather than misbehaving later.

### IAM

The pod identity (or the role named by `assumeRoleArn`) needs to read the schema:

```json
{
  "Effect": "Allow",
  "Action": [
    "glue:GetSchemaVersion",
    "glue:GetSchemaByDefinition"
  ],
  "Resource": "*"
}
```

Scope `Resource` to your registry and schema ARNs rather than `"*"` where you can. Credentials follow
the same model as the source: IRSA / Pod Identity preferred, environment-variable credentials
supported. See [credentials management](../../credentials-management/protecting-credentials.md).

### Example

1. Register your Avro schema in the Glue registry, and note its schema name and version.

2. Create the AWS credentials secret, then deploy:

   ```bash
   kubectl apply -f manifests/avro-glue-producer-config.yaml
   kubectl apply -f manifests/avro-glue-producer-pipeline.yaml
   ```

3. Consume the topic with the [Glue Avro source](../../source/avro-glue/avro-glue-source.md) to confirm
   the records decode.

To additionally encrypt the payload, see
[publish envelope-encrypted payloads](../envelope-encryption/encrypting-sink.md).

### Choose MonoVertex

Although we use a Pipeline to demonstrate, it is highly recommended to use MonoVertex when you don't
need the intermediate buffering a Pipeline provides.
