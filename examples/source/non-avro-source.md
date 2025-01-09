# Read from a topic with no schema or JSON schema registered

### Introduction

This document demonstrates how to read messages from a topic that either has no schema or a JSON schema registered.
Kafka source will de-serialize the message using the byte array de-serializer. For the key, string de-serializer
`org.apache.kafka.common.serialization.StringDeserializer` is used. For the value, byte array de-serializer
`io.confluent.kafka.serializers.ByteArrayDeserializer`.

Current Limitations:

* For topics with JSON schema or NO schema, there is NO schema validation for the messages read from the topic.

### Example

In this example, we create a pipeline that reads a topic `numagen-raw` with no schema registered for the value of the
message and writes the messages to the builtin log sink.

#### Pre-requisite

Create a topic called `numagen-raw` in your Kafka cluster.

Produce some messages to the `numagen-avro` topic. A sample message

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

Create a config map with the following configurations:

```yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: raw-consumer-config
data:
  consumer.properties: |
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
    # group.id is required for consumer clients
    group.id=group1
  user.configuration: |
    topicName: numagen-raw
    groupId: group1
    schemaType: raw
```

`consumer.properties`: [properties](https://kafka.apache.org/documentation/#consumerconfigs) to configure the consumer.
`user.configuration`: User configurations for the source vertex. The configurations include `topicName`, `groupId` and
`schemaType`, which is the Kafka topic name, consumer group id and schema type respectively. The `schemaType` is set to
`raw` to indicate that there is no schema registered for the topic.

Deploy the ConfigMap to the Kubernetes cluster.

#### Create the pipeline

Create the pipeline with Kafka source and Numaflow builtin log sink. Configure the Kafka source with the ConfigMap
created in the previous step.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: raw-consumer
spec:
  vertices:
    - name: in
      volumes:
        - name: kafka-config-volume
          configMap:
            name: raw-consumer-config
            items:
              - key: user.configuration
                path: user.configuration.yaml
              - key: consumer.properties
                path: consumer.properties
      scale:
        min: 1
        max: 1
      source:
        udsource:
          container:
            image: quay.io/numaio/numaflow-java/kafka-java:v0.3.0
            args: [ "--spring.config.location=file:/conf/user.configuration.yaml", "--consumer.properties.path=/conf/consumer.properties" ]
            imagePullPolicy: Always
            volumeMounts:
              - name: kafka-config-volume
                mountPath: /conf
    - name: sink
      scale:
        min: 1
        max: 1
      sink:
        log:
          { }
  edges:
    - from: in
      to: sink
```

Please make sure that the args list under the source vertex matches the file paths in the ConfigMap.

#### Observe the log sink

Wait for the pipeline to be up and running. You should see the messages being consumed from the Kafka topic and logged
in sink vertex.

```
Payload -  {"Data":{"value":1736439076729944818},"Createdts":1736439076729944818}
```