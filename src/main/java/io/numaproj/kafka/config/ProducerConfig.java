package io.numaproj.kafka.config;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.numaproj.kafka.common.EnvVarInterpolator;
import io.numaproj.kafka.encryption.EncryptingSerializer;
import io.numaproj.kafka.encryption.EncryptionProps;
import io.numaproj.kafka.encryption.EnvelopeEncryptionFactory;
import io.numaproj.kafka.encryption.PayloadEncryptor;
import io.numaproj.kafka.schema.ConfluentRegistry;
import io.numaproj.kafka.schema.GlueRegistry;
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
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Factory for Kafka producer clients and schema registry */
@Slf4j
public class ProducerConfig {

  private static final String SCHEMA_REGISTRY_TYPE_KEY = "schema.registry.type";
  private static final String SCHEMA_REGISTRY_TYPE_CONFLUENT = "confluent";
  private static final String SCHEMA_REGISTRY_TYPE_GLUE = "glue";

  private static final String REGION_KEY = "region";
  private static final String REGISTRY_NAME_KEY = "registry.name";
  private static final String DEFAULT_REGISTRY_NAME = "default-registry";
  private static final String ASSUME_ROLE_ARN_KEY = "assumeRoleArn";

  private final String producerPropertiesFilePath;

  public ProducerConfig(String producerPropertiesFilePath) {
    this.producerPropertiesFilePath = producerPropertiesFilePath;
  }

  private Properties loadProps() throws IOException {
    Properties props = new Properties();
    try (InputStream is = new FileInputStream(this.producerPropertiesFilePath)) {
      props.load(is);
    }
    EnvVarInterpolator.interpolate(props);
    loadCredentialProperties(props);
    return props;
  }

  /**
   * The configured schema registry type, {@code confluent} by default. Read by the application so it
   * can pick the matching {@link Registry} without opening the properties file itself.
   */
  public String schemaRegistryType() throws IOException {
    return loadProps().getProperty(SCHEMA_REGISTRY_TYPE_KEY, SCHEMA_REGISTRY_TYPE_CONFLUENT);
  }

  // Kafka producer client to publish raw data in byte array format to Kafka
  // It is used when the destination topic has no schema or json schema
  public KafkaProducer<String, byte[]> kafkaByteArrayProducer() throws IOException {
    log.info(
        "Instantiating the Kafka byte array producer from the producer properties file path: {}",
        this.producerPropertiesFilePath);
    Properties props = loadProps();
    // never register schemas on behalf of the user
    props.put("auto.register.schemas", "false");

    // Build the (optional) payload encryptor, then build and configure the value serializer instance
    // and wrap it when encryption is enabled.
    PayloadEncryptor encryptor = EnvelopeEncryptionFactory.fromProps(props);
    Map<String, Object> configs = toSerializerConfigs(props);
    ByteArraySerializer valueSerializer = new ByteArraySerializer();
    valueSerializer.configure(configs, false);
    StringSerializer keySerializer = new StringSerializer();
    keySerializer.configure(configs, true);

    // strip kafka-java-managed keys as the last step before instantiating the client
    stripManagedProps(props);
    return new KafkaProducer<>(
        props, keySerializer, wrapWithEncryption(valueSerializer, encryptor));
  }

  // Kafka producer client for Avro
  public KafkaProducer<String, GenericRecord> kafkaAvroProducer() throws IOException {
    log.info(
        "Instantiating the Kafka Avro producer from the producer properties file path: {}",
        this.producerPropertiesFilePath);
    Properties props = loadProps();

    String registryType =
        props.getProperty(SCHEMA_REGISTRY_TYPE_KEY, SCHEMA_REGISTRY_TYPE_CONFLUENT);
    log.info("Schema registry type: {}", registryType);
    boolean useGlueSchemaRegistry = SCHEMA_REGISTRY_TYPE_GLUE.equalsIgnoreCase(registryType);
    if (useGlueSchemaRegistry) {
      // The Data Platform wire format is zlib-compressed Avro in a Glue frame; the serializer writes
      // compression flag 0x05 for ZLIB. Defaults only — an operator can still override them.
      props.putIfAbsent("dataFormat", "AVRO");
      props.putIfAbsent("compression", "ZLIB");
      // Glue defaults to SPECIFIC_RECORD; the sink hands it a GenericRecord.
      props.putIfAbsent("avroRecordType", "GENERIC_RECORD");
    }
    // never register schemas on behalf of the user; a schema that is not already registered must
    // fail rather than be created implicitly (Confluent and Glue spell this differently)
    props.put("auto.register.schemas", "false");
    props.putIfAbsent("schemaAutoRegistrationEnabled", "false");

    PayloadEncryptor encryptor = EnvelopeEncryptionFactory.fromProps(props);
    Map<String, Object> configs = toSerializerConfigs(props);

    Serializer<Object> avroSerializer =
        useGlueSchemaRegistry ? new GlueSchemaRegistryKafkaSerializer() : new KafkaAvroSerializer();
    avroSerializer.configure(configs, false);
    StringSerializer keySerializer = new StringSerializer();
    keySerializer.configure(configs, true);

    @SuppressWarnings("unchecked")
    Serializer<GenericRecord> valueSerializer =
        (Serializer<GenericRecord>) (Serializer<?>) avroSerializer;

    // strip kafka-java-managed keys as the last step before instantiating the client
    stripManagedProps(props);
    return new KafkaProducer<>(
        props, keySerializer, wrapWithEncryption(valueSerializer, encryptor));
  }

  // Schema registry client
  // It is used when the destination topic has json or avro schema
  public SchemaRegistryClient schemaRegistryClient() throws IOException {
    Properties props = loadProps();
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

  /**
   * The schema registry the configured {@code schema.registry.type} selects. The caller owns the
   * returned registry and must close it.
   */
  public Registry schemaRegistry() throws IOException {
    Properties props = loadProps();
    String registryType =
        props.getProperty(SCHEMA_REGISTRY_TYPE_KEY, SCHEMA_REGISTRY_TYPE_CONFLUENT);
    if (SCHEMA_REGISTRY_TYPE_GLUE.equalsIgnoreCase(registryType)) {
      return GlueRegistry.create(
          props.getProperty(REGION_KEY),
          props.getProperty(REGISTRY_NAME_KEY, DEFAULT_REGISTRY_NAME),
          props.getProperty(ASSUME_ROLE_ARN_KEY));
    }
    return new ConfluentRegistry(schemaRegistryClient());
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
   * not passed to Kafka clients: {@code schema.registry.type} and the {@code
   * payload.envelope.encryption.*} family.
   */
  private static void stripManagedProps(Properties props) {
    props.remove(SCHEMA_REGISTRY_TYPE_KEY);
    props.keySet().removeIf(k -> k instanceof String s && s.startsWith(EncryptionProps.PREFIX));
  }

  /**
   * Wraps the given value serializer with envelope encryption when an encryptor is present; otherwise
   * returns it unchanged. The wrapper goes on the outside, so encryption is the final step.
   */
  @VisibleForTesting
  static <T> Serializer<T> wrapWithEncryption(Serializer<T> serializer, PayloadEncryptor encryptor) {
    return encryptor == null ? serializer : new EncryptingSerializer<>(serializer, encryptor);
  }

  private static Map<String, Object> toSerializerConfigs(Properties props) {
    Map<String, Object> configs = new HashMap<>();
    for (String name : props.stringPropertyNames()) {
      configs.put(name, props.getProperty(name));
    }
    return configs;
  }
}
