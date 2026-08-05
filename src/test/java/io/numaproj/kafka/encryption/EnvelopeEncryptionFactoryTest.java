package io.numaproj.kafka.encryption;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class EnvelopeEncryptionFactoryTest {

  private static final String VALID_ARN = "arn:aws:kms:us-east-1:123456789012:key/abcd-1234";

  @Test
  void disabledWhenKeyArnAbsent() {
    assertNull(EnvelopeEncryptionFactory.fromProps(new Properties()));
  }

  @Test
  void disabledWhenKeyArnBlank() {
    Properties props = new Properties();
    props.setProperty(EncryptionProps.KEY_ARN, "   ");
    assertNull(EnvelopeEncryptionFactory.fromProps(props));
  }

  @Test
  void failsFastOnMalformedKeyArn() {
    Properties props = new Properties();
    props.setProperty(EncryptionProps.KEY_ARN, "not-an-arn");
    assertThrows(IllegalArgumentException.class, () -> EnvelopeEncryptionFactory.fromProps(props));
  }

  @Test
  void failsFastOnBareAlias() {
    Properties props = new Properties();
    props.setProperty(EncryptionProps.KEY_ARN, "alias/interim/autotask/kafka/swm1/prod");
    assertThrows(IllegalArgumentException.class, () -> EnvelopeEncryptionFactory.fromProps(props));
  }

  @Test
  void failsFastOnNonPositiveTtl() {
    Properties props = new Properties();
    props.setProperty(EncryptionProps.KEY_ARN, VALID_ARN);
    props.setProperty(EncryptionProps.DEK_TTL_MS, "0");

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> EnvelopeEncryptionFactory.fromProps(props));
    assertTrue(e.getMessage().contains("must be a positive long"));
  }

  @Test
  void failsFastOnNonNumericTtl() {
    Properties props = new Properties();
    props.setProperty(EncryptionProps.KEY_ARN, VALID_ARN);
    props.setProperty(EncryptionProps.DEK_TTL_MS, "an-hour-ish");

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> EnvelopeEncryptionFactory.fromProps(props));
    assertTrue(e.getMessage().contains("must be a long"));
  }

  @Test
  void buildsEncryptorWhenKeyArnIsSet() {
    Properties props = new Properties();
    props.setProperty(EncryptionProps.KEY_ARN, VALID_ARN);

    // Building the KMS client makes no network call.
    PayloadEncryptor encryptor = EnvelopeEncryptionFactory.fromProps(props);
    try {
      assertTrue(encryptor != null);
    } finally {
      encryptor.close();
    }
  }
}
