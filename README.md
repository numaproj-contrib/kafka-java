# Numaflow Kafka Sourcer/Sinker

## Overview

Numaflow Kafka Sourcer/Sinker is a [Numaflow](https://numaflow.numaproj.io/) user-defined source/sink connector for
Apache Kafka. It allows you to read/write data from/to Kafka using Numaflow.

## Usage

### Source Vertex

TODO

### Sink Vertex

Kafka Sinker reads data from the upstream vertices and writes the data to a Kafka topic. It uses the schema defined in
schema registry to parse, serialize and publish messages to the target Kafka topic. Below is an example of how to use.

In this example, we use Numaflow built-in generator source to generate data, and a built-in cat UDF to pass the data to
our Kafka sinker.

Data produced by the generator source is a nested struct with a Data field and a Createdts field. A sample data looks

```json
{
  "Data": {
    "value": 1734412253251741190
  },
  "Createdts": 1734412253251741190
}
```

#### Pre-requisites

* In your Kafka cluster, create a topic named `numagen`.
* In your Schema Registry, create a schema named `numagen-value` with the following AVRO schema matching the data
  structure:

```json
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
  "name": "numagen",
  "type": "record"
}
```

#### Steps

1. Configure the sink vertex in a K8s ConfigMap. The configurations include 3 parts: Kafka producer properties, schema
   registry client properties and user configurations. Fill in the configurations according to your Kafka cluster and
   Schema Registry Client, as well as add the topic name in the user configurations.

```yaml
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-config
data:
  producer.properties: |
    # Required connection configs for Kafka producer, consumer, and admin
    bootstrap.servers=localhost:9092
    security.protocol=SASL_SSL
    sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='user' password='password';
    sasl.mechanism=PLAIN
    # Required for correctness in Apache Kafka clients prior to 2.6
    client.dns.lookup=use_all_dns_ips
    # Best practice for higher availability in Apache Kafka clients prior to 3.0
    session.timeout.ms=45000
    # Best practice for Kafka producer to prevent data loss
    acks=all
    # Schema Registry connection configurations
    schema.registry.url=localhost:8081
    basic.auth.credentials.source=USER_INFO
    basic.auth.user.info=a:b
    # Other configurations
    retries=0
    # SerDe
    key.serializer=org.apache.kafka.common.serialization.StringSerializer
    value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
  schema.registry.properties: |
    # Schema Registry connection configurations
    schema.registry.url=localhost:8081
    basic.auth.credentials.source=USER_INFO
    basic.auth.user.info=a:b
  user.configuration: |
    topicName: numagen
```

`producer.properties`: Kafka Producer [properties](https://kafka.apache.org/documentation/#producerconfigs) to configure
the producer.
`schema.registry.properties`: Schema
Registry [properties](https://docs.confluent.io/platform/current/schema-registry/sr-client-configs.html) to
configure the schema registry client.
`user.configuration`: User configurations for the sink vertex. The configurations include topicName, the Kafka topic
name to write data to.

2. Specify the source in a Numaflow pipeline.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: test-pipeline
spec:
  vertices:
    - name: in
      scale:
        min: 1
      source:
        generator:
          rpu: 1
          duration: 1s
    - name: cat
      scale:
        min: 1
      udf:
        builtin:
          name: cat
    - name: sink
      volumes:
        - name: kafka-config-volume
          configMap:
            name: kafka-config
            items:
              - key: user.configuration
                path: user.configuration.yaml
              - key: producer.properties
                path: producer.properties
              - key: schema.registry.properties
                path: schema.registry.properties
      scale:
        min: 1
      limits:
        readBatchSize: 10
      sink:
        udsink:
          container:
            image: quay.io/numaio/numaflow-java/kafka-java:v0.1.0
            args: [ "--spring.config.location=file:/conf/user.configuration.yaml",
                    "--producer.properties.path=/conf/producer.properties",
                    "--schema.registry.properties.path=/conf/schema.registry.properties"
            ]
            imagePullPolicy: Always
            volumeMounts:
              - name: kafka-config-volume
                mountPath: /conf
  edges:
    - from: in
      to: cat
    - from: cat
      to: sink
```

Please make sure that the args list under the sink vertex matches the file paths in the ConfigMap.

3. Apply the ConfigMap and the pipeline, wait for the pipeline to be running. You should see the data being published
   to the Kafka topic.


