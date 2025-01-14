# Publish messages to a topic directly with NO schema used

### Introduction

This document demonstrates how to use `kafka-java` to publish messages to a topic directly, with NO schema used for
validation/serialization.

In `kafka-java`, if no schema information is provided, by default, the producer uses string serializer
`org.apache.kafka.common.serialization.StringSerializer` to serialize the key. For the value,
`org.apache.kafka.common.serialization.ByteArraySerializer`.

### Example

In this example, we create a pipeline that reads from the
builtin [generator source](https://numaflow.numaproj.io/user-guide/sources/generator/) and writes the messages to a
target topic `numagen-raw`, with no schema validation.

#### Pre-requisite

Create a topic called `numagen-raw` in your Kafka cluster.

#### Configure the Kafka producer

Use the example [ConfigMap](raw-producer-config.yaml) to configure the Kafka sinker.

In the ConfigMap:

* `producer.properties` holds the [properties](https://kafka.apache.org/documentation/#producerconfigs) to configure the
  producer.

* `user.configuration` is the user configuration for the sink vertex.
    * `topicName` is the Kafka topic name to write data to.
    * `schemaType` is set to `raw` to indicate no schema is used to validate/serialize the data.

Deploy the ConfigMap to the Kubernetes cluster.

#### Create the pipeline

Use the example [pipeline](raw-producer-pipeline.yaml) to create the pipeline, using the ConfigMap created in the
previous step. Please make sure that the args list under the sink vertex matches the file paths in the ConfigMap.

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

### Choose MonoVertex

Although we use Pipeline to demonstrate, it is highly recommended to use
the [MonoVertex](https://numaflow.numaproj.io/core-concepts/monovertex/) to build your streaming data processing
application on Numaflow. The way you specify the sink specification stays the same.

