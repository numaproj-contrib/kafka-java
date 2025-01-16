# Publish to a topic with an Avro schema registered

### Introduction

This document demonstrates how to publish messages to a topic that has an Avro schema registered. When a topic has an
Avro schema, `kafka-java` producer serializes the value of the message using the schema. For the key, string serializer
`org.apache.kafka.common.serialization.StringSerializer` is used. For the value, confluent Avro serializer
`io.confluent.kafka.serializers.KafkaAvroSerializer`.

Current Limitations:

* The Avro sink assumes the input payload is in json format, it uses the
  `org.apache.avro.io.JsonDecoder` to decode the payload to Avro GenericRecord before sending to the Kafka topic. Please
  consider contributing if you want other formats to be supported. We
  have [an open issue](https://github.com/numaproj-contrib/kafka-java/issues/18) to track the feature.

### Example

In this example, we create a pipeline that reads
from the builtin [generator source](https://numaflow.numaproj.io/user-guide/sources/generator/)  and writes the messages
to a target topic `numagen-avro` with Avro schema `numagen-avro-value` registered for the value of the message.

#### Pre-requisite

Create a topic called `numagen-avro` in your Kafka cluster with the following Avro schema `numagen-avro-value`
registered.

```avroschema
{
  "fields": [
    {
      "name": "Data",
      "type": {
        "fields": [
          {
            "name": "value",
            "type": "long"
          }
        ],
        "name": "Data",
        "type": "record"
      }
    },
    {
      "name": "Createdts",
      "type": "long"
    }
  ],
  "name": "numagen-avro",
  "type": "record"
}
```

#### Configure the Kafka producer

Use the example [ConfigMap](manifests/avro-producer-config.yaml) to configure the Kafka sinker.

In the ConfigMap:

* `producer.properties` holds the [producer properties](https://kafka.apache.org/documentation/#producerconfigs) as well
  as [schema registry properties](https://github.com/confluentinc/schema-registry/blob/master/client/src/main/java/io/confluent/kafka/schemaregistry/client/SchemaRegistryClientConfig.java)
  to configure the producer. Ensure that the schema registry configurations are set because Avro schema is used to
  serialize the data.

* `user.configuration` is the user configuration for the sink vertex.
    * `topicName` is the Kafka topic name to write data to.
    * `schemaType` is set to `avro` to indicate that Avro schema is used to validate and serialize the data.
    * `schemaSubject` is the subject name in the schema registry for the Avro schema.
    * `schemaVersion` is the version of the schema in the schema registry.

Deploy the ConfigMap to the Kubernetes cluster.

#### Create the pipeline

Use the example [pipeline](manifests/avro-producer-pipeline.yaml) to create the pipeline, using the ConfigMap created in
the previous step. Please make sure that the args list under the sink vertex matches the file paths in the ConfigMap.

#### Observe the messages

Wait for the pipeline to be up and running. You can observe the messages in the `numagen-avro` topic. A sample message

```json
{
  "key": "a406ad8d-62b0-4a1d-bd1b-6792d656fbf0",
  "value": {
    "Data": {
      "value": 1736439076729944818
    },
    "Createdts": 1736439076729944818
  }
}
```

### Choose MonoVertex

Although we use Pipeline to demonstrate, it is highly recommended to use
the [MonoVertex](https://numaflow.numaproj.io/core-concepts/monovertex/) to build your streaming data processing
application on Numaflow. The way you specify the sink specification stays the same.