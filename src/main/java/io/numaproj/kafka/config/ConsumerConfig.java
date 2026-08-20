package io.numaproj.kafka.config;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.numaproj.kafka.encryption.DecryptingDeserializer;
import io.numaproj.kafka.encryption.EnvelopeDecryptionFactory;
import io.numaproj.kafka.encryption.PayloadDecryptor;
import java.io.IOException;
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

  private final ClientProps clientProps;

  public ConsumerConfig(String consumerPropertiesFilePath) {
    this.clientProps = new ClientProps(consumerPropertiesFilePath);
  }

  private Properties loadProps() throws IOException {
    return clientProps.load();
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
    log.info("Instantiating the Kafka Avro consumer");
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

    boolean useGlueSchemaRegistry =
        SerializationProps.TYPE_GLUE.equalsIgnoreCase(
            props.getProperty(
                SerializationProps.SCHEMA_REGISTRY_TYPE, SerializationProps.TYPE_CONFLUENT));
    log.info("Using the Glue schema registry: {}", useGlueSchemaRegistry);
    if (useGlueSchemaRegistry) {
      // Required, not merely defaulted: the Glue library supplies no avroRecordType of its own, and
      // the deserializer needs one to choose a datum reader. GENERIC_RECORD is the only workable
      // value here — SPECIFIC_RECORD needs generated classes, and the source forwards
      // GenericRecords. Fixed, not user-configurable.
      props.put(SerializationProps.AVRO_RECORD_TYPE, SerializationProps.AVRO_RECORD_TYPE_GENERIC);
      // kafka-java never registers a schema — a consumer least of all.
      props.put(SerializationProps.GLUE_SCHEMA_AUTO_REGISTRATION, "false");
    }

    // align max.poll.records with the Numaflow batch size so the consumer fetches
    // exactly as many records as the pipeline requests per read cycle
    props.put(MAX_POLL_RECORDS_CONFIG, String.valueOf(batchSize));
    log.info("Setting max.poll.records to {}", batchSize);

    // Build the (optional) payload decryptor, then build and configure the value deserializer
    // instance and wrap it when decryption is enabled.
    PayloadDecryptor decryptor = EnvelopeDecryptionFactory.fromProps(props);
    Map<String, Object> configs = ClientProps.toConfigMap(props);

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
    log.info("Instantiating the Kafka byte array consumer");
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

    PayloadDecryptor decryptor = EnvelopeDecryptionFactory.fromProps(props);
    Map<String, Object> configs = ClientProps.toConfigMap(props);

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
    // strip kafka-java-managed keys as the last step before instantiating the client
    stripManagedProps(props);
    return KafkaAdminClient.create(props);
  }

  private static void stripManagedProps(Properties props) {
    ClientProps.stripManagedProps(props);
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
