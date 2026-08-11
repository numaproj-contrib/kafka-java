package io.numaproj.kafka.config;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.exception.GlueSchemaRegistryIncompatibleDataException;
import io.numaproj.kafka.common.BadRecordException;
import io.numaproj.kafka.encryption.DecryptingDeserializer;
import io.numaproj.kafka.encryption.EnvelopeDecryptionFactory;
import io.numaproj.kafka.encryption.PayloadDecryptor;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.kms.model.KmsInternalException;

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

  @SuppressWarnings("unchecked")
  private static Deserializer<Object> mockDeserializer() {
    return mock(Deserializer.class);
  }

  @Test
  public void wrapWithBadRecordTranslation_glueIncompatibleData_translatesToBadRecordException() {
    Deserializer<Object> delegate = mockDeserializer();
    when(delegate.deserialize(any(), any()))
        .thenThrow(new GlueSchemaRegistryIncompatibleDataException("bad header byte"));
    Deserializer<Object> wrapped = ConsumerConfig.wrapWithBadRecordTranslation(delegate);

    assertThrows(BadRecordException.class, () -> wrapped.deserialize("topic", new byte[0]));
  }

  @Test
  public void wrapWithBadRecordTranslation_glueApiFailure_propagatesUntranslated() {
    Deserializer<Object> delegate = mockDeserializer();
    when(delegate.deserialize(any(), any()))
        .thenThrow(
            new AWSSchemaRegistryException(
                "registry unreachable",
                KmsInternalException.builder().message("slow").build()));
    Deserializer<Object> wrapped = ConsumerConfig.wrapWithBadRecordTranslation(delegate);

    assertThrows(AWSSchemaRegistryException.class, () -> wrapped.deserialize("topic", new byte[0]));
  }

  @Test
  public void wrapWithBadRecordTranslation_glueExceptionWithNoSdkCause_translatesToBadRecordException() {
    Deserializer<Object> delegate = mockDeserializer();
    when(delegate.deserialize(any(), any()))
        .thenThrow(new AWSSchemaRegistryException("some other schema registry failure"));
    Deserializer<Object> wrapped = ConsumerConfig.wrapWithBadRecordTranslation(delegate);

    assertThrows(BadRecordException.class, () -> wrapped.deserialize("topic", new byte[0]));
  }

  @Test
  public void wrapWithBadRecordTranslation_serializationException_translatesToBadRecordException() {
    Deserializer<Object> delegate = mockDeserializer();
    when(delegate.deserialize(any(), any())).thenThrow(new SerializationException("malformed"));
    Deserializer<Object> wrapped = ConsumerConfig.wrapWithBadRecordTranslation(delegate);

    assertThrows(BadRecordException.class, () -> wrapped.deserialize("topic", new byte[0]));
  }
}
