package io.numaproj.kafka.crypto;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class EnvelopeDecryptionFactoryTest {

  private static final String VALID_ARN = "arn:aws:kms:us-east-1:123456789012:key/abcd-1234";

  @Test
  void disabledWhenKeyArnAbsent() {
    assertNull(EnvelopeDecryptionFactory.fromProps(new Properties()));
  }

  @Test
  void disabledWhenKeyArnBlank() {
    Properties props = new Properties();
    props.setProperty(EnvelopeDecryptionFactory.KEY_ARN, "   ");
    assertNull(EnvelopeDecryptionFactory.fromProps(props));
  }

  @Test
  void failsFastOnMalformedArn() {
    Properties props = new Properties();
    props.setProperty(EnvelopeDecryptionFactory.KEY_ARN, "not-an-arn");
    assertThrows(
        IllegalArgumentException.class, () -> EnvelopeDecryptionFactory.fromProps(props));
  }

  @Test
  void failsFastOnAliasArn() {
    Properties props = new Properties();
    props.setProperty(
        EnvelopeDecryptionFactory.KEY_ARN,
        "arn:aws:kms:us-east-1:123456789012:alias/my-key");
    assertThrows(
        IllegalArgumentException.class, () -> EnvelopeDecryptionFactory.fromProps(props));
  }

  @Test
  void failsFastOnNonPositiveTtl() {
    Properties props = new Properties();
    props.setProperty(EnvelopeDecryptionFactory.KEY_ARN, VALID_ARN);
    props.setProperty(EnvelopeDecryptionFactory.DEK_CACHE_TTL_MS, "0");
    assertThrows(
        IllegalArgumentException.class, () -> EnvelopeDecryptionFactory.fromProps(props));
  }

  @Test
  void buildsDecryptorForValidArn() {
    Properties props = new Properties();
    props.setProperty(EnvelopeDecryptionFactory.KEY_ARN, VALID_ARN);
    PayloadDecryptor decryptor = EnvelopeDecryptionFactory.fromProps(props);
    assertNotNull(decryptor);
    decryptor.close(); // releases the KMS client
  }

  @Test
  void arnValidation() {
    assertTrue(EnvelopeDecryptionFactory.isValidKmsKeyArn(VALID_ARN));
    assertFalse(
        EnvelopeDecryptionFactory.isValidKmsKeyArn(
            "arn:aws:kms:us-east-1:123456789012:alias/my-key"));
    assertFalse(
        EnvelopeDecryptionFactory.isValidKmsKeyArn("arn:aws:s3:::my-bucket")); // wrong service
    assertFalse(EnvelopeDecryptionFactory.isValidKmsKeyArn("garbage"));
    assertFalse(
        EnvelopeDecryptionFactory.isValidKmsKeyArn(
            "arn:aws:kms::123456789012:key/abcd")); // no region
  }
}
