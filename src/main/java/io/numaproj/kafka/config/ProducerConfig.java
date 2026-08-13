package io.numaproj.kafka.config;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.numaproj.kafka.common.aws.AwsCredentials;
import io.numaproj.kafka.encryption.EncryptingSerializer;
import io.numaproj.kafka.encryption.EnvelopeEncryptionFactory;
import io.numaproj.kafka.encryption.PayloadEncryptor;
import io.numaproj.kafka.schema.ConfluentRegistry;
import io.numaproj.kafka.schema.GlueRegistry;
import io.numaproj.kafka.schema.Registry;
import java.io.IOException;
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

  private final ClientProps clientProps;

  public ProducerConfig(String producerPropertiesFilePath) {
    this.clientProps = new ClientProps(producerPropertiesFilePath);
  }

  private Properties loadProps() throws IOException {
    return clientProps.load();
  }

  /**
   * Whether the configured {@code schema.registry.type} is Glue ({@code confluent} by default). Read
   * by the application so it can pick the matching {@link Registry} without opening the properties
   * file itself.
   */
  public boolean isGlueSchemaRegistry() throws IOException {
    return SerializationProps.TYPE_GLUE.equalsIgnoreCase(
        loadProps()
            .getProperty(
                SerializationProps.SCHEMA_REGISTRY_TYPE, SerializationProps.TYPE_CONFLUENT));
  }

  // Kafka producer client to publish raw data in byte array format to Kafka
  // It is used when the destination topic has no schema or json schema
  public KafkaProducer<String, byte[]> kafkaByteArrayProducer() throws IOException {
    log.info("Instantiating the Kafka byte array producer");
    // The Glue keys settled here are inert on this path — json and raw values are produced as
    // bytes, and the ByteArraySerializer reads none of them — but the auto-registration guarantee
    // holds everywhere, so every producer path goes through the same call.
    return buildProducer(applySerializerConfigs(), new ByteArraySerializer());
  }

  // Kafka producer client for Avro
  public KafkaProducer<String, GenericRecord> kafkaAvroProducer() throws IOException {
    log.info("Instantiating the Kafka Avro producer");
    Properties props = applySerializerConfigs();

    boolean useGlueSchemaRegistry = isGlueSchemaRegistry();
    log.info("Using the Glue schema registry: {}", useGlueSchemaRegistry);

    Serializer<Object> avroSerializer =
        useGlueSchemaRegistry ? new GlueSchemaRegistryKafkaSerializer() : new KafkaAvroSerializer();
    @SuppressWarnings("unchecked")
    Serializer<GenericRecord> valueSerializer =
        (Serializer<GenericRecord>) (Serializer<?>) avroSerializer;
    return buildProducer(props, valueSerializer);
  }

  /**
   * The producer properties with the serializer configs kafka-java owns settled on top, on every
   * producer path. {@code put}, not {@code putIfAbsent}, for everything except {@code compression}:
   * those keys are fixed rather than defaulted, so a value in the properties file cannot break the
   * contract the sink relies on — the serializer is handed a {@code GenericRecord}, and no schema is
   * ever registered on the user's behalf. In-frame compression is the one the user does own;
   * kafka-java only defaults it.
   *
   * <p>kafka-java is a connector: it reads schemas, it never creates them. Auto-registration is
   * therefore disabled unconditionally rather than only for the Avro paths — a schema definition
   * that is not already in the registry must fail, not be created implicitly.
   */
  @VisibleForTesting
  Properties applySerializerConfigs() throws IOException {
    Properties props = loadProps();
    if (isGlueSchemaRegistry()) {
      props.put(SerializationProps.DATA_FORMAT, SerializationProps.DATA_FORMAT_AVRO);
      // The Glue serializer never reads avroRecordType — it picks the datum writer from the object
      // it is handed, which here is always a GenericRecord. Pinned anyway because the Glue config
      // object still parses the value with AvroRecordType.valueOf, so a stray or misspelled one
      // would fail sink startup over a setting the sink does not use.
      props.put(SerializationProps.AVRO_RECORD_TYPE, SerializationProps.AVRO_RECORD_TYPE_GENERIC);
      // ZLIB writes frame compression flag 0x05; the Glue serializer's own default is NONE.
      props.putIfAbsent(SerializationProps.COMPRESSION, SerializationProps.COMPRESSION_ZLIB);
    }
    // Confluent and Glue spell auto-registration differently; disable both.
    props.put(SerializationProps.CONFLUENT_AUTO_REGISTER_SCHEMAS, "false");
    props.put(SerializationProps.GLUE_SCHEMA_AUTO_REGISTRATION, "false");
    return props;
  }

  private <T> KafkaProducer<String, T> buildProducer(Properties props, Serializer<T> rawValueSerializer) {
    PayloadEncryptor encryptor = EnvelopeEncryptionFactory.fromProps(props);
    Map<String, Object> configs = ClientProps.toConfigMap(props);
    rawValueSerializer.configure(configs, false);
    StringSerializer keySerializer = new StringSerializer();
    keySerializer.configure(configs, true);
    stripManagedProps(props);
    return new KafkaProducer<>(props, keySerializer, wrapWithEncryption(rawValueSerializer, encryptor));
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
    if (isGlueSchemaRegistry()) {
      return GlueRegistry.create(
          props.getProperty(SerializationProps.REGION),
          props.getProperty(
              SerializationProps.REGISTRY_NAME, SerializationProps.DEFAULT_REGISTRY_NAME),
          props.getProperty(AwsCredentials.ASSUME_ROLE_ARN));
    }
    return new ConfluentRegistry(schemaRegistryClient());
  }

  /**
   * Remove keys that are not Kafka client configs so they are not passed to Kafka clients: the
   * kafka-java-managed keys every path strips, plus the Glue/AWS serializer keys, which the
   * serializer instance has already been configured with — leaving them in would only make the
   * producer log "supplied but isn't a known config" warnings.
   */
  private static void stripManagedProps(Properties props) {
    ClientProps.stripManagedProps(props);
    props.remove(SerializationProps.REGION);
    props.remove(SerializationProps.REGISTRY_NAME);
    props.remove(SerializationProps.DATA_FORMAT);
    props.remove(SerializationProps.COMPRESSION);
    props.remove(SerializationProps.AVRO_RECORD_TYPE);
    props.remove(SerializationProps.GLUE_SCHEMA_AUTO_REGISTRATION);
    props.remove(SerializationProps.CONFLUENT_AUTO_REGISTER_SCHEMAS);
  }

  /**
   * Wraps the given value serializer with envelope encryption when an encryptor is present; otherwise
   * returns it unchanged. The wrapper goes on the outside, so encryption is the final step.
   */
  @VisibleForTesting
  static <T> Serializer<T> wrapWithEncryption(Serializer<T> serializer, PayloadEncryptor encryptor) {
    return encryptor == null ? serializer : new EncryptingSerializer<>(serializer, encryptor);
  }
}
