# Read from a topic with an Avro schema registered in AWS Glue Schema Registry

### Introduction

This document demonstrates how to read messages from a Kafka topic whose Avro schema is registered in
[AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html).

When `schema.registry.type=glue` is set in `consumer.properties`, the source uses
`GlueSchemaRegistryKafkaDeserializer` instead of the Confluent deserializer. The schema version is embedded
in each message by the producer, so no schema subject or version needs to be configured on the consumer side.
Deserialized Avro `GenericRecord` values are re-encoded as JSON before being forwarded to the next vertex.

Current limitations:

- The Avro source encodes deserialized records as JSON using `org.apache.avro.io.JsonEncoder`. Other formats are 
  not supported ([issue #19](https://github.com/numaproj-contrib/kafka-java/issues/19)).

### Prerequisites

1. An AWS Glue registry and schema already created. The schema must be registered by the producer before
   the consumer starts — `schemaAutoRegistrationEnabled=false` is the recommended setting for consumers.

2. AWS credentials available to the pod with at least the following IAM permissions:
   ```json
   {
     "Effect": "Allow",
     "Action": [
       "glue:GetRegistry",
       "glue:GetSchema",
       "glue:GetSchemaVersion",
       "glue:QuerySchemaVersionMetadata"
     ],
     "Resource": "*"
   }
   ```

3. A Kafka topic whose messages were produced using `GlueSchemaRegistryKafkaSerializer`.

### Example

This example creates a pipeline that reads from a topic `my-topic` and writes the messages to the
built-in log sink.

#### Step 1 — Configure AWS credentials

On EKS,
prefer [IRSA](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html)
or [Pod Identity](https://docs.aws.amazon.com/eks/latest/userguide/pod-identities.html) over static
credentials.

If you need to use environment variables, ensure they are not hardcoded in the pipeline manifest:

```bash
kubectl create secret generic aws-creds \
  --from-literal=AWS_ACCESS_KEY_ID=<your-access-key-id> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<your-secret-access-key> \
  --from-literal=AWS_SESSION_TOKEN=<your-session-token>
```

For temporary credentials (e.g. from `aws sts assume-role`), include `AWS_SESSION_TOKEN`. 

#### Step 2 — Configure the Kafka consumer

Use the example [ConfigMap](manifests/avro-glue-consumer-config.yaml) as a starting point, filling in
the placeholders:

| Placeholder | Description |
|---|---|
| `bootstrap.servers` | Kafka broker address(es) |
| `security.protocol`, `sasl.*` | Authentication settings for your Kafka cluster |
| `registry.name` | Name of your Glue registry |
| `topicName` | Kafka topic to consume from |

Glue-specific properties:

| Property | Required | Default | Description |
|---|---|---|---|
| `schema.registry.type` | Yes | `confluent` | Set to `glue` to enable Glue Schema Registry |
| `region` | Yes | — | AWS region of the Glue registry (e.g. `us-east-1`) |
| `registry.name` | No | `default-registry` | Name of the Glue registry |
| `schemaAutoRegistrationEnabled` | No | `false` | Whether to auto-create schemas; leave `false` for consumers |
| `aws.role.arn` | No | — | IAM role ARN to assume before connecting to Glue (see below) |
| `cacheSize` | No | `200` | Max number of schema versions to cache in memory |
| `timeToLiveMillis` | No | `86400000` | Schema cache TTL in milliseconds (default 24 h) |

Of these, schema.registry.type and aws.role.arn are managed by kafka-java. The other properties are passed on to the 
Glue deserializer.

Deploy the ConfigMap:

```bash
kubectl apply -f avro-glue-consumer-config.yaml
```

#### Step 3 — Create the pipeline

Use the example [pipeline](manifests/avro-glue-consumer-pipeline.yaml), which references both the
ConfigMap and the `aws-creds` Secret created above:

```bash
kubectl apply -f avro-glue-consumer-pipeline.yaml
```

#### Step 4 — Observe the log sink

Once the pipeline is running you should see JSON-encoded Avro records in the sink vertex logs:

```
Payload -  {"Name":"John","Age":30}
```

### Assuming an IAM role

If the pod's base credentials do not have direct Glue access, you can instruct the consumer to assume 
an IAM role by adding `aws.role.arn` to `consumer.properties`:

```properties
aws.role.arn=arn:aws:iam::123456789012:role/my-glue-role
```

The consumer calls AWS STS `AssumeRole` at startup using the pod's existing credentials, then uses the
returned temporary credentials to authenticate with Glue. Credentials are automatically refreshed
5 minutes before they expire — no pod restart is required.

The pod's base credentials must have `sts:AssumeRole` permission on the target role, and the target
role's trust policy must allow the pod's identity to assume it.

### Choose MonoVertex

Although a Pipeline is used in this example, it is recommended to use
[MonoVertex](https://numaflow.numaproj.io/core-concepts/monovertex/) for production workloads. The
source specification is identical.

### Protect your credentials

See [credentials management](../../credentials-management/protecting-credentials.md) for best practices
around storing Kafka and AWS credentials.
