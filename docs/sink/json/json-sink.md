# Publish to a topic with a JSON schema registered

### Introduction

This document demonstrates how to publish messages to a topic that has a JSON schema registered. When a topic has a
JSON schema, Kafka sink will validate the message against the schema and then use the byte array serializer to
serialize the value of the message. For the key, string serializer
`org.apache.kafka.common.serialization.StringSerializer` is used. For the value,
`org.apache.kafka.common.serialization.ByteArraySerializer`.

Current Limitations:

* The JSON sink assumes the schema follows the default subject naming strategy (TopicNameStrategy) in the schema
  registry, meaning the name of the schema matches `{TopicName}-value`.

### Example

In this example, we create a pipeline that reads from the builtin generator and write the messages to a target topic
`numagen-json` with JSON schema `numagen-json-value` registered for the value of the message.

#### Pre-requisite

Create a topic called `numagen-json` in your Kafka cluster with the following JSON schema `numagen-json-value`
registered.

```json
{
  "$id": "test-id",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "schema for the topic numagen-json",
  "properties": {
    "Createdts": {
      "format": "int64",
      "type": "integer"
    },
    "Data": {
      "additionalProperties": false,
      "properties": {
        "value": {
          "format": "int64",
          "type": "integer"
        }
      },
      "type": "object"
    }
  },
  "required": [
    "Data",
    "Createdts"
  ],
  "title": "numagen-json",
  "type": "object"
}
```

#### Configure the Kafka producer

Create a ConfigMap with the following configurations:

```yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: json-producer-config
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
    # Other configurations
    retries=0
  user.configuration: |
    topicName: numagen-json
    schemaType: json
```

`producer.properties` holds the [properties](https://kafka.apache.org/documentation/#producerconfigs) to configure the
producer.

`user.configuration` is the user configuration for the sink vertex. The configuration includes topicName, the Kafka
topic name to write data to, and schemaType. The `schemaType` is set to `json` to indicate that JSON schema is used to
validate the data before publishing.

Deploy the ConfigMap to the Kubernetes cluster.

#### Create the pipeline

Create the pipeline with Numaflow builtin generator and Kafka sink. Configure the Kafka sink with the ConfigMap created
in the previous step.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: json-producer
spec:
  vertices:
    - name: in
      scale:
        min: 1
        max: 1
      source:
        generator:
          rpu: 1
          duration: 5s
    - name: sink
      volumes:
        - name: kafka-config-volume
          configMap:
            name: json-producer-config
            items:
              - key: user.configuration
                path: user.configuration.yaml
              - key: producer.properties
                path: producer.properties
      scale:
        min: 1
        max: 1
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

Wait for the pipeline to be up and running. You can observe the messages in the `numagen-json` topic. A sample message

```json
{
  "key": "a406ad8d-62b0-4a1d-bd1b-6792d656fbf0",
  "value": {
    "Data": {
      "value": "1736448031039625125"
    },
    "Createdts": "1736448031039625125"
  }
}
```