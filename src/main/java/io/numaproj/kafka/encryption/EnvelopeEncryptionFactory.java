package io.numaproj.kafka.encryption;

import io.numaproj.kafka.encryption.aws.KmsDekGenerator;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds a {@link PayloadEncryptor} from producer properties, or returns {@code null} when encryption
 * is disabled. Presence of the AWS KMS key ARN is the enable switch; a malformed ARN fails fast at
 * startup.
 *
 * <p>The mirror of {@link EnvelopeDecryptionFactory}, and backend-agnostic in the same way: it reads
 * the config surface and wires codec + process-lifetime reuse + generator. AWS-specific knowledge
 * (ARN validity, region, client lifecycle) lives in {@link KmsDekGenerator}.
 */
@Slf4j
public final class EnvelopeEncryptionFactory {

  private EnvelopeEncryptionFactory() {}

  /**
   * @return an encryptor when the key ARN is set, otherwise {@code null} (encryption disabled)
   * @throws IllegalArgumentException if the key ARN is set but malformed (fail-fast at startup)
   */
  public static PayloadEncryptor fromProps(Properties props) {
    String keyArn = props.getProperty(EncryptionProps.KEY_ARN);
    if (keyArn == null || keyArn.isBlank()) {
      return null;
    }
    String assumeRoleArn = props.getProperty(EncryptionProps.ASSUME_ROLE_ARN);
    KmsDekGenerator kmsGenerator = KmsDekGenerator.create(keyArn.trim(), assumeRoleArn);
    log.info("Payload envelope encryption enabled (aws-kms)");
    // One DEK per process lifetime: generated on first use, rotated by restart/redeploy.
    return new PayloadEncryptor(
        new JsonEnvelopeCodec(), new ProcessLifetimeDekGenerator(kmsGenerator));
  }
}
