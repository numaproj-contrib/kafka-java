# Publish to a topic using a JSON schema to validate the data

### Introduction

This document demonstrates how to publish messages to a topic, using a JSON schema to validate before sending.
When a JSON schema is provided, `kafka-java` producer validates the value of the message against the schema and then
uses the byte array serializer `org.apache.kafka.common.serialization.ByteArraySerializer` to serialize the value of the
message. For the key, string serializer `org.apache.kafka.common.serialization.StringSerializer`.

### Example

In this example, we create a pipeline that reads from the
builtin [generator source](https://numaflow.numaproj.io/user-guide/sources/generator/) and write the messages to a
target topic `numagen-json` with JSON schema `numagen-json-value` registered for the value of the message.

#### Pre-requisite

Create a topic called `numagen-json` in your Kafka cluster with the following JSON schema `numagen-json-value`
registered in Confluent schema registry.

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

Use the example [ConfigMap](manifests/json-producer-config.yaml) to configure the Kafka sinker.

In the ConfigMap:

* `producer.properties` holds the [properties](https://kafka.apache.org/documentation/#producerconfigs) as well
  as [schema registry properties](https://github.com/confluentinc/schema-registry/blob/master/client/src/main/java/io/confluent/kafka/schemaregistry/client/SchemaRegistryClientConfig.java)
  to configure the producer. Ensure that the schema registry configurations are set because JSON schema is used to
  validate the data.

* `user.configuration` is the user configuration for the sink vertex.
    * `topicName` is the Kafka topic name to write data to.
    * `schemaType` is set to `json` to indicate that JSON schema is used to validate the data before publishing.
    * `schemaSubject` is the subject name in the schema registry for the JSON schema.
    * `schemaVersion` is the version of the schema in the schema registry.

Deploy the ConfigMap to the Kubernetes cluster.

#### Create the pipeline

Use the example [pipeline](manifests/json-producer-pipeline.yaml) to create the pipeline, using the ConfigMap created in
the previous step. Please make sure that the args list under the sink vertex matches the file paths in the ConfigMap.

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

### Choose MonoVertex

Although we use Pipeline to demonstrate, it is highly recommended to use
the [MonoVertex](https://numaflow.numaproj.io/core-concepts/monovertex/) to build your streaming data processing
application on Numaflow. The way you specify the sink specification stays the same.

### Protect your credentials

In the example, the `producer.properties` contains the credentials. Please
see [credentials management](../../credentials-management/protecting-credentials.md) to protect your credentials.