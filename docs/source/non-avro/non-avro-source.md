# Read from a topic with no schema or JSON schema registered

### Introduction

This document demonstrates how to read messages from a topic that either has no schema or a JSON schema registered.
Kafka source will de-serialize the value of the message using the byte array de-serializer. For the key, string
de-serializer `org.apache.kafka.common.serialization.StringDeserializer` is used. For the value, byte array
de-serializer `io.confluent.kafka.serializers.ByteArrayDeserializer`.

Current Limitations:

* For topics with JSON schema or NO schema, there is NO schema validation for the messages read from the topic. The
  Kafka source assumes the message is valid.

### Example

In this example, we create a pipeline that reads a topic `numagen-raw` with no schema registered for the value of the
message and writes the messages to the builtin log sink.

#### Pre-requisite

Create a topic called `numagen-raw` in your Kafka cluster.

Produce some messages to the `numagen-raw` topic. A sample message

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

Use the example [ConfigMap](manifests/raw-consumer-config.yaml) to configure the Kafka sourcer.

* `consumer.properties` holds the [properties](https://kafka.apache.org/documentation/#consumerconfigs) to configure the
  consumer. Ensure that the schema registry configurations are set because Avro schema is used to de-serialize the data.

* `user.configuration` is the user configuration for the source vertex.
    * `topicName` is the Kafka topic name to read data from.
    * `groupId` is the consumer group id.
    * `schemaType` is set to `raw` to indicate that there is no schema registered for the topic. (You can also set the
      `schemaType` to `json` if the topic has a JSON schema registered).

Deploy the ConfigMap to the Kubernetes cluster.

#### Create the pipeline

Create the pipeline with Kafka source and Numaflow builtin log sink. Configure the Kafka source with the ConfigMap
created in the previous step.

Use the example [pipeline](manifests/raw-consumer-pipeline.yaml) to create the pipeline, using the ConfigMap created in
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
application on Numaflow. The way you specify the sink specification stays the same.