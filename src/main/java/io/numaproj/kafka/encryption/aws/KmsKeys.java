package io.numaproj.kafka.encryption.aws;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;

/**
 * KMS key ARN handling, shared by the DEK unwrapper (source) and generator (sink).
 *
 * <p>A full key ARN is required rather than an alias, so the region can be derived from the key
 * itself and the same identifier can be pinned as {@code KeyId} on the KMS call.
 */
final class KmsKeys {

  private KmsKeys() {}

  /**
   * Validates {@code keyArn} and returns the region it names.
   *
   * @throws IllegalArgumentException if it is not a well-formed KMS key ARN (fail-fast at startup);
   *     bare aliases are rejected
   */
  static Region validateAndGetRegion(String keyArn) {
    if (!isValidKmsKeyArn(keyArn)) {
      throw new IllegalArgumentException(
          "Invalid KMS key ARN (expected arn:aws:kms:<region>:<account>:key/<id>): " + keyArn);
    }
    return Region.of(Arn.fromString(keyArn).region().orElseThrow());
  }

  static boolean isValidKmsKeyArn(String candidate) {
    try {
      Arn arn = Arn.fromString(candidate);
      return KmsClient.SERVICE_NAME.equals(arn.service())
          && arn.region().filter(r -> !r.isBlank()).isPresent()
          && arn.resourceAsString().startsWith("key/");
    } catch (RuntimeException e) {
      return false;
    }
  }
}
