package io.numaproj.kafka.encryption.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

class KmsKeysTest {

  private static final String KEY_ARN = "arn:aws:kms:us-east-1:123456789012:key/abcd-1234";

  @Test
  void acceptsFullKeyArn() {
    assertTrue(KmsKeys.isValidKmsKeyArn(KEY_ARN));
  }

  @Test
  void rejectsAliasesAndMalformedArns() {
    // A bare alias carries no region, so it cannot be used to derive one.
    assertFalse(KmsKeys.isValidKmsKeyArn("alias/my-key"));
    assertFalse(
        KmsKeys.isValidKmsKeyArn("arn:aws:kms:us-east-1:123456789012:alias/my-key")); // alias arn
    assertFalse(KmsKeys.isValidKmsKeyArn("arn:aws:s3:::my-bucket")); // wrong service
    assertFalse(KmsKeys.isValidKmsKeyArn("arn:aws:kms::123456789012:key/abcd")); // no region
    assertFalse(KmsKeys.isValidKmsKeyArn("garbage"));
    assertFalse(KmsKeys.isValidKmsKeyArn(null));
  }

  @Test
  void derivesRegionFromArn() {
    assertEquals(Region.US_EAST_1, KmsKeys.validateAndGetRegion(KEY_ARN));
  }

  @Test
  void validateRejectsMalformedArn() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> KmsKeys.validateAndGetRegion("not-an-arn"));
    assertTrue(e.getMessage().contains("Invalid KMS key ARN"));
  }
}
