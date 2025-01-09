# Publish to a topic with no schema registered

### Introduction

This document demonstrates how to publish messages to a topic with no schema registered. When a topic has no schema, we
assume no data validation is required before sending. Hence, for the key, the native
`org.apache.kafka.common.serialization.StringSerializer` is used. For the value,
`org.apache.kafka.common.serialization.ByteArraySerializer`.

### Example

In this example, we create a pipeline that reads from the builtin generator and write the messages to a target topic
`numagen-raw` with no schema registered.

#### Pre-requisite

Create a topic called `numagen-raw` in your Kafka cluster with no schema registered.

#### Configure the Kafka producer

Create a config map with the following configurations:

```yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-producer-config
data:
  producer.properties: |
    # Required connection configs for Kafka producer
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
  user.configuration: |
    topicName: numagen-raw
    schemaType: raw
```

// TODO - what exactly is the right link?
`producer.properties`: [properties](https://kafka.apache.org/documentation/#producerconfigs) to configure the producer.
`user.configuration`: User configurations for the sink vertex. The configurations include topicName, the Kafka topic
name to write data to, and schemaType. The `schemaType` is set to `raw` to indicate no schema. Deploy the ConfigMap to
the Kubernetes cluster.

#### Create the pipeline

Create the pipeline with numaflow built-in generator and Kafka sink. Configure the Kafka sink with the ConfigMap created
in the previous step.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: raw-producer
spec:
  vertices:
    - name: in
      scale:
        min: 1
      source:
        generator:
          rpu: 1
          duration: 5s
    - name: sink
      volumes:
        - name: kafka-config-volume
          configMap:
            name: raw-producer-config
            items:
              - key: user.configuration
                path: user.configuration.yaml
              - key: producer.properties
                path: producer.properties
      scale:
        min: 1
        max: 6
      sink:
        udsink:
          container:
            image: quay.io/numaio/numaflow-java/kafka-java:v0.3.0
            args: [ "--spring.config.location=file:/conf/user.configuration.yaml", "--producer.properties.path=/conf/producer.properties" ]
            imagePullPolicy: Always
            volumeMounts:
              - name: kafka-config-volume
                mountPath: /conf
  edges:
    - from: in
      to: sink
```

Please make sure that the args list under the sink vertex matches the file paths in the ConfigMap.

#### Observe the messages

Wait for the pipeline to be up and running. You can observe the messages in the `numagen-raw` topic. A sample message

```json
{
  "key": "a406ad8d-62b0-4a1d-bd1b-6792d656fbf0",
  "value": {
    "Data": {
      "value": "1736434270629262337"
    },
    "Createdts": "1736434270629262337"
  }
}
```

