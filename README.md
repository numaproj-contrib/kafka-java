# Numaflow Kafka Sourcer/Sinker

## Overview

Numaflow Kafka Sourcer/Sinker is a [Numaflow](https://numaflow.numaproj.io/) user-defined source/sink connector for
Apache Kafka. It allows you to read/write data from/to a Kafka topic using Numaflow. Integration with Confluent Schema
Registry is also supported.

## Use Cases

### Read data from Kafka

Use Case 1: Read data from Kafka with an Avro schema registered in the Confluent Schema Registry. See an
example [here](docs/source/avro/avro-source.md).

Use Case 2: Read data from Kafka with no schema or JSON schema registered in the Confluent Schema Registry. See an
example [here](docs/source/non-avro/non-avro-source.md).

### Write data to Kafka

Use Case 3: Write data to Kafka with an Avro schema registered in the Confluent Schema Registry. See an
example [here](docs/sink/avro/avro-sink.md).

Use Case 4: Write data to Kafka with a JSON schema registered in the Confluent Schema Registry. See an
example [here](docs/sink/json/json-sink.md).

Use Case 5: Write data to Kafka with no schema. See an example [here](docs/sink/no-schema/no-schema-sink.md).