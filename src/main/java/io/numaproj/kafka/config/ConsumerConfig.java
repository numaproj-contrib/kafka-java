package io.numaproj.kafka.config;

import io.numaproj.kafka.common.AwsIamRoleUtils;
import io.numaproj.kafka.common.EnvVarInterpolator;
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
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/** Factory for Kafka consumer clients and admin client */
@Slf4j
public class ConsumerConfig {

  private static final String SCHEMA_REGISTRY_TYPE_KEY = "schema.registry.type";
  private static final String SCHEMA_REGISTRY_TYPE_CONFLUENT = "confluent";
  private static final String SCHEMA_REGISTRY_TYPE_GLUE = "glue";

  private final String consumerPropertiesFilePath;

  public ConsumerConfig(String consumerPropertiesFilePath) {
    this.consumerPropertiesFilePath = consumerPropertiesFilePath;
  }

  private Properties loadProps() throws IOException {
    Properties props = new Properties();
    try (InputStream is = new FileInputStream(this.consumerPropertiesFilePath)) {
      props.load(is);
    }
    EnvVarInterpolator.interpolate(props);
    return props;
  }

  /**
   * Provides the consumer group ID from consumer.properties file. This is the single source of
   * truth for group.id configuration.
   */
  public String consumerGroupId() throws IOException {
    Properties props = loadProps();

    var groupId =
        props.getOrDefault(GROUP_ID_CONFIG, null);
    if (groupId == null || StringUtils.isBlank((String) groupId)) {
      throw new IllegalArgumentException("group.id is mandatory for Kafka consumer");
    }
    log.info("Consumer group ID from consumer.properties: {}", groupId);
    return (String) groupId;
  }

  // Kafka Avro consumer client
  public KafkaConsumer<String, GenericRecord> kafkaAvroConsumer(int batchSize) throws IOException {
    log.info(
        "Instantiating the Kafka Avro consumer from the consumer properties file: {}",
        this.consumerPropertiesFilePath);
    Properties props = loadProps();
    // disable auto commit, numaflow data forwarder takes care of committing offsets
    if (props.getProperty(
                ENABLE_AUTO_COMMIT_CONFIG)
            != null
        && Boolean.parseBoolean(
            props.getProperty(
                ENABLE_AUTO_COMMIT_CONFIG))) {
      log.info("Overwriting enable.auto.commit to false.");
    }
    props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
    // ensure consumer group id is present
    var groupId =
        props.getOrDefault(GROUP_ID_CONFIG, null);
    if (groupId == null || StringUtils.isBlank((String) groupId)) {
      throw new IllegalArgumentException("group.id is mandatory for Kafka consumer");
    }

    // override the deserializer
    // TODO - warning message if user sets a different deserializer
    props.put(
        KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");

    String registryType = props.getProperty(SCHEMA_REGISTRY_TYPE_KEY, SCHEMA_REGISTRY_TYPE_CONFLUENT);
    props.remove(SCHEMA_REGISTRY_TYPE_KEY); // no need to pass it on to KafkaConsumer
    log.info("Schema registry type: {}", registryType);

    if (SCHEMA_REGISTRY_TYPE_GLUE.equalsIgnoreCase(registryType)) {
      AwsIamRoleUtils.assumeRoleIfConfigured(props);
      props.put(VALUE_DESERIALIZER_CLASS_CONFIG,
          "com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer");
      // Glue defaults to SPECIFIC_RECORD; force GENERIC_RECORD unless the user overrides it
      props.putIfAbsent("avroRecordType", "GENERIC_RECORD");
    } else {
      props.put(VALUE_DESERIALIZER_CLASS_CONFIG,
          "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    }

    // align max.poll.records with the Numaflow batch size so the consumer fetches
    // exactly as many records as the pipeline requests per read cycle
    props.put(
        MAX_POLL_RECORDS_CONFIG,
        String.valueOf(batchSize));
    log.info("Setting max.poll.records to {}", batchSize);

    // set credential properties from environment variable
    String credentialProperties = System.getenv("KAFKA_CREDENTIAL_PROPERTIES");
    if (credentialProperties != null && !credentialProperties.isEmpty()) {
      StringReader sr = new StringReader(credentialProperties);
      props.load(sr);
      sr.close();
      EnvVarInterpolator.interpolate(props);
    }
    return new KafkaConsumer<>(props);
  }

  // Kafka byte array consumer client
  public KafkaConsumer<String, byte[]> kafkaByteArrayConsumer(int batchSize) throws IOException {
    log.info(
        "Instantiating the Kafka byte array consumer from the consumer properties file: {}",
        this.consumerPropertiesFilePath);
    Properties props = loadProps();
    // disable auto commit, numaflow data forwarder takes care of committing offsets
    if (props.getProperty(
                ENABLE_AUTO_COMMIT_CONFIG)
            != null
        && Boolean.parseBoolean(
            props.getProperty(
                ENABLE_AUTO_COMMIT_CONFIG))) {
      log.info("Overwriting enable.auto.commit to false.");
    }
    props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
    // ensure consumer group id is present
    var groupId =
        props.getOrDefault(GROUP_ID_CONFIG, null);
    if (groupId == null || StringUtils.isBlank((String) groupId)) {
      throw new IllegalArgumentException("group.id is mandatory for Kafka consumer");
    }

    // override the deserializer
    // TODO - warning message if user sets a different deserializer
    props.put(
        KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(
        VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    // align max.poll.records with the Numaflow batch size so the consumer fetches
    // exactly as many records as the pipeline requests per read cycle
    props.put(
        MAX_POLL_RECORDS_CONFIG,
        String.valueOf(batchSize));
    log.info("Setting max.poll.records to {}", batchSize);

    // set credential properties from environment variable
    String credentialProperties = System.getenv("KAFKA_CREDENTIAL_PROPERTIES");
    if (credentialProperties != null && !credentialProperties.isEmpty()) {
      StringReader sr = new StringReader(credentialProperties);
      props.load(sr);
      sr.close();
      EnvVarInterpolator.interpolate(props);
    }
    return new KafkaConsumer<>(props);
  }

  // AdminClient is used to retrieve the number of pending messages.
  // Currently, it shares the same properties file with Kafka consumer client.
  // TODO - consider having a separate properties file for admin client.
  // Admin client should be able to serve both consumer and producer,
  // and it does not need all the properties that consumer client needs.
  public AdminClient kafkaAdminClient() throws IOException {
    Properties props = loadProps();
    // set credential properties from environment variable
    String credentialProperties = System.getenv("KAFKA_CREDENTIAL_PROPERTIES");
    if (credentialProperties != null && !credentialProperties.isEmpty()) {
      StringReader sr = new StringReader(credentialProperties);
      props.load(sr);
      sr.close();
      EnvVarInterpolator.interpolate(props);
    }
    return KafkaAdminClient.create(props);
  }
}
