package io.numaproj.kafka.config;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.numaproj.kafka.common.EnvVarInterpolator;
import io.numaproj.kafka.encryption.DecryptingDeserializer;
import io.numaproj.kafka.encryption.EnvelopeDecryptionFactory;
import io.numaproj.kafka.encryption.PayloadDecryptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;

/** Factory for Kafka consumer clients and admin client */
@Slf4j
public class ConsumerConfig {

  private static final String SCHEMA_REGISTRY_TYPE_KEY = "schema.registry.type";
  private static final String SCHEMA_REGISTRY_TYPE_CONFLUENT = "confluent";
  private static final String SCHEMA_REGISTRY_TYPE_GLUE = "glue";

  // Prefix for kafka-java-managed payload-envelope-decryption keys; consumed internally and stripped
  // before the props are handed to KafkaConsumer.
  private static final String ENCRYPTION_PROP_PREFIX = "payload.envelope.encryption.";

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

    String registryType =
        props.getProperty(SCHEMA_REGISTRY_TYPE_KEY, SCHEMA_REGISTRY_TYPE_CONFLUENT);
    log.info("Schema registry type: {}", registryType);
    boolean useGlueSchemaRegistry = SCHEMA_REGISTRY_TYPE_GLUE.equalsIgnoreCase(registryType);
    if (useGlueSchemaRegistry) {
      // Glue defaults to SPECIFIC_RECORD; force GENERIC_RECORD unless the user overrides it
      props.putIfAbsent("avroRecordType", "GENERIC_RECORD");
    }

    // align max.poll.records with the Numaflow batch size so the consumer fetches
    // exactly as many records as the pipeline requests per read cycle
    props.put(MAX_POLL_RECORDS_CONFIG, String.valueOf(batchSize));
    log.info("Setting max.poll.records to {}", batchSize);

    // set credential properties from environment variable
    loadCredentialProperties(props);

    // Build the (optional) payload decryptor, then build and configure the value deserializer
    // instance and wrap it when decryption is enabled.
    PayloadDecryptor decryptor = EnvelopeDecryptionFactory.fromProps(props);
    Map<String, Object> configs = toDeserializerConfigs(props);

    Deserializer<Object> avroDeserializer =
        useGlueSchemaRegistry
            ? new GlueSchemaRegistryKafkaDeserializer()
            : new KafkaAvroDeserializer();
    avroDeserializer.configure(configs, false);
    StringDeserializer keyDeserializer = new StringDeserializer();
    keyDeserializer.configure(configs, true);

    @SuppressWarnings("unchecked")
    Deserializer<GenericRecord> valueDeserializer =
        (Deserializer<GenericRecord>) (Deserializer<?>) avroDeserializer;

    // strip kafka-java-managed keys as the last step before instantiating the client
    stripManagedProps(props);
    return new KafkaConsumer<>(props, keyDeserializer, wrapWithDecryption(valueDeserializer, decryptor));
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

    // align max.poll.records with the Numaflow batch size so the consumer fetches
    // exactly as many records as the pipeline requests per read cycle
    props.put(MAX_POLL_RECORDS_CONFIG, String.valueOf(batchSize));
    log.info("Setting max.poll.records to {}", batchSize);

    // set credential properties from environment variable
    loadCredentialProperties(props);

    PayloadDecryptor decryptor = EnvelopeDecryptionFactory.fromProps(props);
    Map<String, Object> configs = toDeserializerConfigs(props);

    ByteArrayDeserializer byteArrayDeserializer = new ByteArrayDeserializer();
    byteArrayDeserializer.configure(configs, false);
    StringDeserializer keyDeserializer = new StringDeserializer();
    keyDeserializer.configure(configs, true);

    // strip kafka-java-managed keys as the last step before instantiating the client
    stripManagedProps(props);
    return new KafkaConsumer<>(props, keyDeserializer, wrapWithDecryption(byteArrayDeserializer, decryptor));
  }

  // AdminClient is used to retrieve the number of pending messages.
  // Currently, it shares the same properties file with Kafka consumer client.
  // TODO - consider having a separate properties file for admin client.
  // Admin client should be able to serve both consumer and producer,
  // and it does not need all the properties that consumer client needs.
  public AdminClient kafkaAdminClient() throws IOException {
    Properties props = loadProps();
    // set credential properties from environment variable
    loadCredentialProperties(props);
    // strip kafka-java-managed keys as the last step before instantiating the client
    stripManagedProps(props);
    return KafkaAdminClient.create(props);
  }

  /** Merge credential properties supplied via the KAFKA_CREDENTIAL_PROPERTIES env var. */
  private static void loadCredentialProperties(Properties props) throws IOException {
    String credentialProperties = System.getenv("KAFKA_CREDENTIAL_PROPERTIES");
    if (credentialProperties != null && !credentialProperties.isEmpty()) {
      try (StringReader sr = new StringReader(credentialProperties)) {
        props.load(sr);
      }
      EnvVarInterpolator.interpolate(props);
    }
  }

  /**
   * Remove kafka-java-managed keys (consumed internally, not real Kafka client configs) so they are
   * not passed to Kafka clients: {@code schema.registry.type} and the
   * {@code payload.envelope.encryption.*} family.
   */
  private static void stripManagedProps(Properties props) {
    props.remove(SCHEMA_REGISTRY_TYPE_KEY);
    props.keySet().removeIf(k -> k instanceof String s && s.startsWith(ENCRYPTION_PROP_PREFIX));
  }

  private static Map<String, Object> toDeserializerConfigs(Properties props) {
    Map<String, Object> configs = new HashMap<>();
    for (String name : props.stringPropertyNames()) {
      configs.put(name, props.getProperty(name));
    }
    return configs;
  }

  /**
   * Wraps the given value deserializer with envelope decryption when a decryptor is present;
   * otherwise returns it unchanged.
   */
  @VisibleForTesting
  static <T> Deserializer<T> wrapWithDecryption(
      Deserializer<T> deserializer, PayloadDecryptor decryptor) {
    return decryptor == null ? deserializer : new DecryptingDeserializer<>(deserializer, decryptor);
  }
}
