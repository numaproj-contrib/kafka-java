# Read from a topic with an Avro schema registered

### Introduction

This document demonstrates how to read messages from a topic that has an Avro schema registered. When a topic has an
Avro schema, Kafka source will de-serialize the value of the message using the schema. For the key, string de-serializer
`org.apache.kafka.common.serialization.StringDeserializer` is used. For the value, confluent Avro de-serializer
`io.confluent.kafka.serializers.KafkaAvroDeserializer`.

Current Limitations:

* The Avro source assumes the de-serialized message is in JSON format. It uses `org.apache.Avro.io.JsonEncoder` to
  encode the de-serialized Avro GenericRecord to a byte array before sending to the next vertex.
* The Avro source assumes the schema follows the default subject naming strategy (TopicNameStrategy) in the schema
  registry, meaning the name of the schema matches `{TopicName}-value`.

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

Create a ConfigMap with the following configurations:

```yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: avro-consumer-config
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
    # Schema Registry connection configurations
    # Consumer client uses the schema registry to get the schema for the data in the topic and de-serialize the data
    schema.registry.url=[placeholder]
    basic.auth.credentials.source=[placeholder]
    basic.auth.user.info=[placeholder]
    # Best practice for Kafka producer to prevent data loss
    acks=all
    # Other configurations
    retries=0
    # group.id is required for consumer clients
    group.id=group1
  user.configuration: |
    topicName: numagen-avro
    groupId: group1
    schemaType: avro
```

`consumer.properties` holds the [properties](https://kafka.apache.org/documentation/#consumerconfigs) as well
as [schema registry properties](https://github.com/confluentinc/schema-registry/blob/master/client/src/main/java/io/confluent/kafka/schemaregistry/client/SchemaRegistryClientConfig.java)
to configure the consumer. Ensure that the schema registry configurations are set because Avro schema is used to
de-serialize the data.

`user.configuration` is the user configuration for the source vertex. The configuration includes `topicName`, `groupId`
and `schemaType`, which is the Kafka topic name, consumer group id and schema type respectively. The `schemaType` is set
to `avro` to indicate that Avro schema is used to de-serialize the data.

Deploy the ConfigMap to the Kubernetes cluster.

#### Create the pipeline

Create the pipeline with Kafka source and Numaflow builtin log sink. Configure the Kafka source with the ConfigMap
created in the previous step.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: avro-consumer
spec:
  vertices:
    - name: in
      volumes:
        - name: kafka-config-volume
          configMap:
            name: avro-consumer-config
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

Please make sure that the args list under the source vertex container specification matches the file paths in the
ConfigMap.

#### Observe the log sink

Wait for the pipeline to be up and running. You should see the messages being consumed from the Kafka topic and logged
in sink vertex.

```
Payload -  {"Data":{"value":1736439076729944818},"Createdts":1736439076729944818}
```