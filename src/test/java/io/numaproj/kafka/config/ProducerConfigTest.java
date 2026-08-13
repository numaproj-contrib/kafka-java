package io.numaproj.kafka.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.numaproj.kafka.encryption.EncryptingSerializer;
import io.numaproj.kafka.encryption.PayloadEncryptor;
import io.numaproj.kafka.schema.ConfluentRegistry;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@Slf4j
@ExtendWith(MockitoExtension.class)
public class ProducerConfigTest {

  private static ProducerConfig configFor(String resource) {
    return new ProducerConfig(
        Objects.requireNonNull(ProducerConfigTest.class.getClassLoader().getResource(resource))
            .getPath());
  }

  private ProducerConfig underTest() {
    return configFor("producer.properties");
  }

  @Test
  public void kafkaAvroProducer_initializeSuccess() throws Exception {
    try (var producer = underTest().kafkaAvroProducer()) {
      assertNotNull(producer);
    }
  }

  @Test
  public void kafkaByteArrayProducer_initializeSuccess() throws Exception {
    try (var producer = underTest().kafkaByteArrayProducer()) {
      assertNotNull(producer);
    }
  }

  @Test
  public void schemaRegistryClient_initializeSuccess() throws Exception {
    assertNotNull(underTest().schemaRegistryClient());
  }

  @Test
  public void schemaRegistryType_defaultsToConfluent() throws Exception {
    assertFalse(underTest().isGlueSchemaRegistry());
  }

  @Test
  public void schemaRegistry_confluentByDefault() throws Exception {
    var registry = underTest().schemaRegistry();
    assertInstanceOf(ConfluentRegistry.class, registry);
    registry.close();
  }

  @Test
  public void glueRegistryType_avroProducerInitializeSuccess() throws Exception {
    // Constructing the Glue serializer makes no AWS call.
    ProducerConfig glueConfig = configFor("producer.properties.glue");
    assertTrue(glueConfig.isGlueSchemaRegistry());
    try (var producer = glueConfig.kafkaAvroProducer()) {
      assertNotNull(producer);
    }
  }

  @Test
  public void producer_encryptionEnabled_initializeSuccess() throws Exception {
    // Building the KMS client makes no network call.
    try (var producer = configFor("producer.properties.encrypted").kafkaAvroProducer()) {
      assertNotNull(producer);
    }
  }

  @Test
  public void producer_encryptionMalformedArn_failsFast() {
    ProducerConfig badConfig = configFor("producer.properties.encrypted.badarn");
    assertThrows(IllegalArgumentException.class, badConfig::kafkaAvroProducer);
    assertThrows(IllegalArgumentException.class, badConfig::kafkaByteArrayProducer);
  }

  @Test
  public void applySerializerConfigs_overridesTheKeysKafkaJavaOwns() throws Exception {
    Properties props = configFor("producer.properties.glue.userkeys").applySerializerConfigs();

    assertEquals("GENERIC_RECORD", props.getProperty(SerializationProps.AVRO_RECORD_TYPE));
    assertEquals("AVRO", props.getProperty(SerializationProps.DATA_FORMAT));
    assertEquals("false", props.getProperty(SerializationProps.GLUE_SCHEMA_AUTO_REGISTRATION));
    assertEquals("false", props.getProperty(SerializationProps.CONFLUENT_AUTO_REGISTER_SCHEMAS));
    // Compression is the user's to choose, so it is only defaulted.
    assertEquals("ZLIB", props.getProperty(SerializationProps.COMPRESSION));
  }

  @Test
  public void applySerializerConfigs_keepsTheConfiguredCompression() throws Exception {
    Properties props = configFor("producer.properties.glue.nocompression").applySerializerConfigs();

    assertEquals("NONE", props.getProperty(SerializationProps.COMPRESSION));
  }

  @Test
  public void applySerializerConfigs_confluentPathLeavesGlueKeysUnset() throws Exception {
    Properties props = underTest().applySerializerConfigs();

    assertNull(props.getProperty(SerializationProps.COMPRESSION));
    assertNull(props.getProperty(SerializationProps.AVRO_RECORD_TYPE));
    assertEquals("false", props.getProperty(SerializationProps.CONFLUENT_AUTO_REGISTER_SCHEMAS));
  }

  @Test
  public void wrapWithEncryption_wrapsOnlyWhenEncryptorPresent() {
    Serializer<byte[]> delegate = new ByteArraySerializer();

    assertSame(delegate, ProducerConfig.wrapWithEncryption(delegate, null));

    PayloadEncryptor encryptor = mock(PayloadEncryptor.class);
    assertInstanceOf(
        EncryptingSerializer.class, ProducerConfig.wrapWithEncryption(delegate, encryptor));
  }
}
