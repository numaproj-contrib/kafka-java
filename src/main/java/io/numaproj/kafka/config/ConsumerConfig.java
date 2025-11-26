package io.numaproj.kafka.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/** Beans used by Kafka sourcer */
@Slf4j
@Configuration
@ComponentScan(basePackages = "io.numaproj.kafka.consumer")
@ConditionalOnProperty(name = "consumer.properties.path")
public class ConsumerConfig {

  @Value("${consumer.properties.path:NA}")
  private String consumerPropertiesFilePath;

  // package-private constructor. this is for unit test only.
  ConsumerConfig(@Value("${consumer.properties.path:NA}") String consumerPropertiesFilePath) {
    this.consumerPropertiesFilePath = consumerPropertiesFilePath;
  }

  /**
   * Provides the consumer group ID from consumer.properties file. This is the single source of
   * truth for group.id configuration.
   */
  @Bean
  public String consumerGroupId() throws IOException {
    Properties props = new Properties();
    InputStream is = new FileInputStream(this.consumerPropertiesFilePath);
    props.load(is);
    is.close();

    var groupId =
        props.getOrDefault(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, null);
    if (groupId == null || StringUtils.isBlank((String) groupId)) {
      throw new IllegalArgumentException("group.id is mandatory for Kafka consumer");
    }
    log.info("Consumer group ID from consumer.properties: {}", groupId);
    return (String) groupId;
  }

  // Kafka Avro consumer client
  @Bean
  @ConditionalOnProperty(name = "schemaType", havingValue = "avro")
  public KafkaConsumer<String, GenericRecord> kafkaAvroConsumer() throws IOException {
    log.info(
        "Instantiating the Kafka Avro consumer from the consumer properties file: {}",
        this.consumerPropertiesFilePath);
    Properties props = new Properties();
    InputStream is = new FileInputStream(this.consumerPropertiesFilePath);
    props.load(is);
    is.close();
    // disable auto commit, numaflow data forwarder takes care of committing offsets
    if (props.getProperty(
                org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
            != null
        && Boolean.parseBoolean(
            props.getProperty(
                org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))) {
      log.info("Overwriting enable.auto.commit to false ");
    }
    props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // ensure consumer group id is present
    var groupId =
        props.getOrDefault(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, null);
    if (groupId == null || StringUtils.isBlank((String) groupId)) {
      throw new IllegalArgumentException("group.id is mandatory for Kafka consumer");
    }

    // override the deserializer
    // TODO - warning message if user sets a different deserializer
    props.put(
        org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(
        org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "io.confluent.kafka.serializers.KafkaAvroDeserializer");

    // set credential properties from environment variable
    String credentialProperties = System.getenv("KAFKA_CREDENTIAL_PROPERTIES");
    if (credentialProperties != null && !credentialProperties.isEmpty()) {
      StringReader sr = new StringReader(credentialProperties);
      props.load(sr);
      sr.close();
    }
    return new KafkaConsumer<>(props);
  }

  // Kafka byte array consumer client
  @Bean
  @ConditionalOnExpression("'${schemaType}'.equals('json') or '${schemaType}'.equals('raw')")
  public KafkaConsumer<String, byte[]> kafkaByteArrayConsumer() throws IOException {
    log.info(
        "Instantiating the Kafka byte array consumer from the consumer properties file: {}",
        this.consumerPropertiesFilePath);
    Properties props = new Properties();
    InputStream is = new FileInputStream(this.consumerPropertiesFilePath);
    props.load(is);
    is.close();
    // disable auto commit, numaflow data forwarder takes care of committing offsets
    if (props.getProperty(
                org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
            != null
        && Boolean.parseBoolean(
            props.getProperty(
                org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))) {
      log.info("Overwriting enable.auto.commit to false ");
    }
    props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // ensure consumer group id is present
    var groupId =
        props.getOrDefault(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, null);
    if (groupId == null || StringUtils.isBlank((String) groupId)) {
      throw new IllegalArgumentException("group.id is mandatory for Kafka consumer");
    }

    // override the deserializer
    // TODO - warning message if user sets a different deserializer
    props.put(
        org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(
        org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    // set credential properties from environment variable
    String credentialProperties = System.getenv("KAFKA_CREDENTIAL_PROPERTIES");
    if (credentialProperties != null && !credentialProperties.isEmpty()) {
      StringReader sr = new StringReader(credentialProperties);
      props.load(sr);
      sr.close();
    }
    return new KafkaConsumer<>(props);
  }

  // AdminClient is used to retrieve the number of pending messages.
  // Currently, it shares the same properties file with Kafka consumer client.
  // TODO - consider having a separate properties file for admin client.
  // Admin client should be able to serve both consumer and producer,
  // and it does not need all the properties that consumer client needs.
  @Bean
  public AdminClient kafkaAdminClient() throws IOException {
    Properties props = new Properties();
    InputStream is = new FileInputStream(this.consumerPropertiesFilePath);
    props.load(is);
    is.close();
    // set credential properties from environment variable
    String credentialProperties = System.getenv("KAFKA_CREDENTIAL_PROPERTIES");
    if (credentialProperties != null && !credentialProperties.isEmpty()) {
      StringReader sr = new StringReader(credentialProperties);
      props.load(sr);
      sr.close();
    }
    return KafkaAdminClient.create(props);
  }
}
