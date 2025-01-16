# Read from a topic with an Avro schema registered

### Introduction

This document demonstrates how to read messages from a topic that has an Avro schema registered. When a topic has an
Avro schema, Kafka source will de-serialize the value of the message using the schema. For the key, string de-serializer
`org.apache.kafka.common.serialization.StringDeserializer` is used. For the value, confluent Avro de-serializer
`io.confluent.kafka.serializers.KafkaAvroDeserializer`.

Current Limitations:

* The Avro source assumes the de-serialized message is in JSON format. It uses `org.apache.Avro.io.JsonEncoder` to
  encode the de-serialized Avro GenericRecord to a byte array before sending to the next vertex. Please
  consider contributing if you want other formats to be supported. We
  have [an open issue](https://github.com/numaproj-contrib/kafka-java/issues/19) to track the feature.

### Example

In this example, we create a pipeline that reads a topic `numagen-avro` with Avro schema `numagen-avro-value` registered
for the value of the message and writes the messages to the builtin log sink.

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

Produce some messages to the `numagen-avro` topic using the Avro schema as serializer. A sample message

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

#### Configure the Kafka consumer

Use the example [ConfigMap](manifests/avro-consumer-config.yaml) to configure the Kafka sourcer.

* `consumer.properties` holds the [properties](https://kafka.apache.org/documentation/#consumerconfigs) as well
  as [schema registry properties](https://github.com/confluentinc/schema-registry/blob/master/client/src/main/java/io/confluent/kafka/schemaregistry/client/SchemaRegistryClientConfig.java)
  to configure the consumer. Ensure that the schema registry configurations are set because Avro schema is used to
  de-serialize the data.

* `user.configuration` is the user configuration for the source vertex.
    * `topicName` is the Kafka topic name to read data from.
    * `groupId` is the consumer group id.
    * `schemaType` is set to `avro` to indicate that Avro schema is used to de-serialize the data.

Deploy the ConfigMap to the Kubernetes cluster.

#### Create the pipeline

Use the example [pipeline](manifests/avro-consumer-pipeline.yaml) to create the pipeline, using the ConfigMap created in
the previous step. Please make sure that the args list under the sink vertex matches the file paths in the ConfigMap.

#### Observe the log sink

Wait for the pipeline to be up and running. You should see the messages being consumed from the Kafka topic and logged
in sink vertex.

```
Payload -  {"Data":{"value":1736439076729944818},"Createdts":1736439076729944818}
```

### Choose MonoVertex

Although we use Pipeline to demonstrate, it is highly recommended to use
the [MonoVertex](https://numaflow.numaproj.io/core-concepts/monovertex/) to build your streaming data processing
application on Numaflow. The way you specify the source specification stays the same.

### Protect your credentials

In the example, the `consumer.properties` contains the credentials. Please
see [credentials management](../../credentials-management/protecting-credentials.md) to protect your credentials.