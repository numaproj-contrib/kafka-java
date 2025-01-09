package io.numaproj.kafka.config;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.numaproj.kafka.schema.ConfluentRegistry;
import io.numaproj.kafka.schema.Registry;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/** Beans used by Kafka producer */
@Slf4j
@Configuration
@ComponentScan(basePackages = {"io.numaproj.kafka.producer", "io.numaproj.kafka.schema"})
@ConditionalOnProperty(name = "producer.properties.path")
public class ProducerConfig {

  @Value("${producer.properties.path:NA}")
  private String producerPropertiesFilePath;

  @Value("${schema.registry.properties.path:NA}")
  private String schemaRegistryPropertiesFilePath;

  // package-private constructor. this is for unit test only.
  ProducerConfig(
      @Value("${producer.properties.path:NA}") String producerPropertiesFilePath,
      @Value("${schema.registry.properties.path:NA}") String schemaRegistryPropertiesFilePath) {
    this.producerPropertiesFilePath = producerPropertiesFilePath;
    this.schemaRegistryPropertiesFilePath = schemaRegistryPropertiesFilePath;
  }

  // Kafka producer client for topics with no schema associated
  // it sends raw messages without serialization
  @Bean
  @ConditionalOnProperty(name = "schemaType", havingValue = "raw")
  public KafkaProducer<String, byte[]> kafkaByteArrayProducer() throws IOException {
    log.info(
        "Instantiating the Kafka raw data producer from the producer properties file path: {}",
        this.producerPropertiesFilePath);
    Properties props = new Properties();
    InputStream is = new FileInputStream(this.producerPropertiesFilePath);
    props.load(is);
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
    // if user sets a different serializer, it will be overwritten with a warning message
    log.info("Kafka raw data producer props read from user input ConfigMap: {}", props);
    is.close();
    return new KafkaProducer<>(props);
  }

  // Kafka producer client for avro
  @Bean
  @ConditionalOnProperty(name = "schemaType", havingValue = "avro")
  public KafkaProducer<String, GenericRecord> kafkaAvroProducer() throws IOException {
    log.info(
        "Instantiating the Kafka avro producer from the producer properties file path: {}",
        this.producerPropertiesFilePath);
    Properties props = new Properties();
    InputStream is = new FileInputStream(this.producerPropertiesFilePath);
    props.load(is);
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
    // if user sets a different serializer, it will be overwritten with a warning message
    log.info("Kafka avro producer props read from user input ConfigMap: {}", props);
    is.close();
    return new KafkaProducer<>(props);
  }

  // Kafka producer client for json
  @Bean
  @ConditionalOnProperty(name = "schemaType", havingValue = "json")
  public KafkaProducer<String, String> kafkaJsonProducer() throws IOException {
    log.info(
        "Instantiating the Kafka json producer from the producer properties file path: {}",
        this.producerPropertiesFilePath);
    Properties props = new Properties();
    InputStream is = new FileInputStream(this.producerPropertiesFilePath);
    props.load(is);
    // override the serializer
    // TODO - warning message if user sets a different serializer
    props.put(
        org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    // TODO - byte array serializer might be better because it is more flexible
    // string will change the message format. e.g., integer to string
    props.put(
        org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");

    // never register schemas on behalf of the user
    props.put("auto.register.schemas", "false");
    // if user sets a different serializer, it will be overwritten with a warning message
    log.info("Kafka json producer props read from user input ConfigMap: {}", props);
    is.close();
    return new KafkaProducer<>(props);
  }

  // Schema registry client
  @Bean
  @ConditionalOnProperty(name = "schemaType", havingValue = "avro")
  public SchemaRegistryClient schemaRegistryClient() throws IOException {
    log.info(
        "Instantiating the Kafka schema registry client from the schema registry properties file path: {}",
        this.schemaRegistryPropertiesFilePath);
    Properties props = new Properties();
    InputStream is = new FileInputStream(this.schemaRegistryPropertiesFilePath);
    props.load(is);
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

  @Bean
  @ConditionalOnProperty(name = "schemaType", havingValue = "avro")
  public Registry schemaRegistry(SchemaRegistryClient schemaRegistryClient) {
    return new ConfluentRegistry(schemaRegistryClient);
  }
}
