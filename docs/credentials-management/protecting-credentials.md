# How to protect your credentials

### Context

The way we manage both Kafka and Schema Registry configurations is through ConfigMap in Kubernetes. Across all the
examples, we declare `producer.properties` and `consumer.properties` in the ConfigMap. These properties contain the
credentials to connect to the Kafka cluster and Schema Registry.

An example of the `producer.properties` is shown below:

```properties
# Required connection configs for Kafka producer, consumer, and admin
bootstrap.servers=[placeholder]
security.protocol=[placeholder]
sasl.jaas.config=[placeholder]
sasl.mechanism=[placeholder]
# Required for correctness in Apache Kafka clients prior to 2.6
client.dns.lookup=use_all_dns_ips
# Best practice for higher availability in Apache Kafka clients prior to 3.0
session.timeout.ms=45000
# Best practice for Kafka producer to prevent data loss
acks=all
# Schema Registry connection configurations
schema.registry.url=[placeholder]
basic.auth.credentials.source=[placeholder]
basic.auth.user.info=[placeholder]
# Other configurations
retries=0
```

The `sasl.jaas.config` and `base.auth.user.info` are the properties that contain the credentials. Having credentials
directly in the ConfigMap is not secure. To protect the credentials, we use Kubernetes secrets.

### Steps to protect your credentials

**Step 1** - Remove the credential properties from the original ConfigMap.

In the example [ConfigMap](./manifests/avro-producer-config.yaml), the credential properties are removed.

**Step 2** - Create a Kubernetes secret with the credential properties.

Data stored in a secret is base64 encoded. Encode the following properties in the secret:

```properties
sasl.jaas.config=[placeholder]
basic.auth.user.info=[placeholder]
```

Generate the base64 encoded string for the properties:

```shell
echo -n 'sasl.jaas.config=[placeholder]
basic.auth.user.info=[placeholder]' | base64
```

which generates the base64 encoded string:

```shell
c2FzbC5qYWFzLmNvbmZpZz1bcGxhY2Vob2xkZXJdCmJhc2ljLmF1dGgudXNlci5pbmZvPVtwbGFjZWhvbGRlcl0=
```

Create a secret using the base64 encoded string, see example [secret](./manifests/kafka-credentials.yaml).

**Step 3** - Create an environment variable in the UDSink vertex called KAFKA_CREDENTIAL_PROPERTIES and set the value to
the secret data. See example [pipeline](./manifests/avro-producer-pipeline.yaml).

**Step 4** - Deploy the pipeline to the Kubernetes cluster.
