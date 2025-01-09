# Publish to a topic with an Avro schema registered

### Introduction

This document demonstrates how to publish messages to a topic that has an Avro schema registered. When a topic has an
Avro schema, Kafka sink will serialize the message using the schema. For the key, the native string serializer
`org.apache.kafka.common.serialization.StringSerializer` is used. For the value, confluent Avro serializer
`io.confluent.kafka.serializers.KafkaAvroSerializer`.

Limitation:

* The avro sinker assumes the input payload is in json format, it uses the
  `org.apache.avro.io.JsonEncoder` to encode the payload before sending to the Kafka topic. It's in our roadmap to
  support other encoders.
* The avro sinker assumes the schema follows the default subject naming strategy (TopicNameStrategy) in the schema
  registry. It's in our roadmap to support other naming strategies.

### Example

In this example, we create a pipeline that reads from the builtin generator and write the messages to a target topic
`numagen-avro` with Avro schema `numagen-avro-value` registered for the value of the message.

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
    # Other configurations
    retries=0
  user.configuration: |
    topicName: numagen-avro
    schemaType: avro
```

// TODO - what exactly is the right link?
`producer.properties`: [properties](https://kafka.apache.org/documentation/#producerconfigs) to configure the producer.
`user.configuration`: User configurations for the sink vertex. The configurations include topicName, the Kafka topic
name to write data to, and schemaType. The `schemaType` is set to `avro` to indicate that avro schema is used to
serialize the data. Deploy the ConfigMap to the Kubernetes cluster.

#### Create the pipeline

Create the pipeline with numaflow built-in generator and Kafka sink. Configure the Kafka sink with the ConfigMap created
in the previous step.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: avro-producer
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
            name: avro-producer-config
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