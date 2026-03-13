package io.numaproj.kafka.config;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.numaproj.kafka.common.EnvVarInterpolator;
import io.numaproj.kafka.schema.ConfluentRegistry;
import io.numaproj.kafka.schema.Registry;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

/** Factory for Kafka producer clients and schema registry */
@Slf4j
public class ProducerConfig {

  private final String producerPropertiesFilePath;

  public ProducerConfig(String producerPropertiesFilePath) {
    this.producerPropertiesFilePath = producerPropertiesFilePath;
  }

  // Kafka producer client to publish raw data in byte array format to Kafka
  // It is used when the destination topic has no schema or json schema
  public KafkaProducer<String, byte[]> kafkaByteArrayProducer() throws IOException {
    log.info(
        "Instantiating the Kafka byte array producer from the producer properties file path: {}",
        this.producerPropertiesFilePath);
    Properties props = new Properties();
    InputStream is = new FileInputStream(this.producerPropertiesFilePath);
    props.load(is);
    EnvVarInterpolator.interpolate(props);
    // override the serializer
    // TODO - warning message if user sets a different serializer
    props.put(
        org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(
        org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    // never register schemas on behalf of the user
    props.put("auto.register.schemas", "false");

    // set credential properties from environment variable
    String credentialProperties = System.getenv("KAFKA_CREDENTIAL_PROPERTIES");
    if (credentialProperties != null && !credentialProperties.isEmpty()) {
      StringReader sr = new StringReader(credentialProperties);
      props.load(sr);
      sr.close();
      EnvVarInterpolator.interpolate(props);
    }
    is.close();
    return new KafkaProducer<>(props);
  }

  // Kafka producer client for Avro
  public KafkaProducer<String, GenericRecord> kafkaAvroProducer() throws IOException {
    log.info(
        "Instantiating the Kafka Avro producer from the producer properties file path: {}",
        this.producerPropertiesFilePath);
    Properties props = new Properties();
    InputStream is = new FileInputStream(this.producerPropertiesFilePath);
    props.load(is);
    EnvVarInterpolator.interpolate(props);
    // override the serializer
    // TODO - warning message if user sets a different serializer
    props.put(
        org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(
        org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "io.confluent.kafka.serializers.KafkaAvroSerializer");
    // never register schemas on behalf of the user
    props.put("auto.register.schemas", "false");

    // set credential properties from environment variable
    String credentialProperties = System.getenv("KAFKA_CREDENTIAL_PROPERTIES");
    if (credentialProperties != null && !credentialProperties.isEmpty()) {
      StringReader sr = new StringReader(credentialProperties);
      props.load(sr);
      sr.close();
      EnvVarInterpolator.interpolate(props);
    }
    is.close();
    return new KafkaProducer<>(props);
  }

  // Schema registry client
  // It is used when the destination topic has json or avro schema
  public SchemaRegistryClient schemaRegistryClient() throws IOException {
    Properties props = new Properties();
    InputStream is = new FileInputStream(this.producerPropertiesFilePath);
    props.load(is);
    EnvVarInterpolator.interpolate(props);

    // set credential properties from environment variable
    String credentialProperties = System.getenv("KAFKA_CREDENTIAL_PROPERTIES");
    if (credentialProperties != null && !credentialProperties.isEmpty()) {
      StringReader sr = new StringReader(credentialProperties);
      props.load(sr);
      sr.close();
      EnvVarInterpolator.interpolate(props);
    }
    String schemaRegistryUrl = props.getProperty("schema.registry.url");
    int identityMapCapacity =
        Integer.parseInt(
            props.getProperty(
                "schema.registry.identity.map.capacity", "100")); // Default to 100 if not specified
    Map<String, String> schemaRegistryClientConfigs = new HashMap<>();
    for (String key : props.stringPropertyNames()) {
      schemaRegistryClientConfigs.put(key, props.getProperty(key));
    }
    return new CachedSchemaRegistryClient(
        schemaRegistryUrl, identityMapCapacity, schemaRegistryClientConfigs);
  }

  public Registry schemaRegistry(SchemaRegistryClient schemaRegistryClient) {
    return new ConfluentRegistry(schemaRegistryClient);
  }
}
