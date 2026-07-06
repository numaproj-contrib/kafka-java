package io.numaproj.kafka.config;

import static org.junit.jupiter.api.Assertions.*;

import io.numaproj.kafka.crypto.DecryptingDeserializer;
import io.numaproj.kafka.crypto.EnvelopeDecryptionFactory;
import io.numaproj.kafka.crypto.PayloadDecryptor;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@Slf4j
@ExtendWith(MockitoExtension.class)
public class ConsumerConfigTest {

  ConsumerConfig underTest;

  @BeforeEach
  public void setUp() {
    underTest =
        new ConsumerConfig(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("consumer/consumer.properties"))
                .getPath());
  }

  @Test
  public void consumer_initializeSuccess() {
    try {
      var kafkaConsumer = underTest.kafkaAvroConsumer(500);
      assertNotNull(kafkaConsumer);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void consumer_groupIdNotSpecified() {
    underTest =
        new ConsumerConfig(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("consumer/consumer.properties.no.group.id"))
                .getPath());
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          underTest.kafkaAvroConsumer(500);
        });
  }

  @Test
  public void consumer_overrideAutoCommitEnableToFalse() {
    // FIXME - figure out a way to verify the override
    underTest =
        new ConsumerConfig(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("consumer/consumer.properties.auto.commit.enabled"))
                .getPath());
    try {
      var kafkaConsumer = underTest.kafkaAvroConsumer(500);
      assertNotNull(kafkaConsumer);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void adminClient_initializeSuccess() {
    try {
      var adminClient = underTest.kafkaAdminClient();
      assertNotNull(adminClient);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void consumerGroupId_success() {
    try {
      var groupId = underTest.consumerGroupId();
      assertEquals("groupId", groupId);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void consumerGroupId_notSpecified() {
    underTest =
        new ConsumerConfig(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("consumer/consumer.properties.no.group.id"))
                .getPath());
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          underTest.consumerGroupId();
        });
  }

  @Test
  public void consumer_glueRegistryType_initializeSuccess() {
    underTest =
        new ConsumerConfig(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("consumer/consumer.properties.glue"))
                .getPath());
    try {
      assertNotNull(underTest.kafkaAvroConsumer(500));
    } catch (Exception e) {
      fail("Failed to initialize Glue-backed Avro consumer: " + e.getMessage());
    }
  }

  @Test
  public void consumer_encryptionEnabled_initializeSuccess() {
    underTest =
        new ConsumerConfig(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("consumer/consumer.properties.encrypted"))
                .getPath());
    try (var kafkaConsumer = underTest.kafkaAvroConsumer(500)) {
      assertNotNull(kafkaConsumer);
    } catch (Exception e) {
      fail("Failed to initialize encryption-enabled Avro consumer: " + e.getMessage());
    }
  }

  @Test
  public void consumer_encryptionMalformedArn_failsFast() {
    underTest =
        new ConsumerConfig(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("consumer/consumer.properties.encrypted.badarn"))
                .getPath());
    assertThrows(IllegalArgumentException.class, () -> underTest.kafkaAvroConsumer(500));
  }

  @Test
  public void wrapWithDecryption_noDecryptor_returnsSameDeserializer() {
    Deserializer<String> deserializer = new StringDeserializer();
    assertSame(deserializer, ConsumerConfig.wrapWithDecryption(deserializer, null));
  }

  @Test
  public void wrapWithDecryption_withDecryptor_wraps() {
    Properties props = new Properties();
    props.setProperty(
        EnvelopeDecryptionFactory.KEY_ARN, "arn:aws:kms:us-east-1:123456789012:key/abcd-1234");
    PayloadDecryptor decryptor = EnvelopeDecryptionFactory.fromProps(props);
    assertNotNull(decryptor);

    Deserializer<String> wrapped =
        ConsumerConfig.wrapWithDecryption(new StringDeserializer(), decryptor);
    assertInstanceOf(DecryptingDeserializer.class, wrapped);
    wrapped.close(); // releases the KMS client held by the decryptor
  }
}
