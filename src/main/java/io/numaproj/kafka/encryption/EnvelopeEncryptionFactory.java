package io.numaproj.kafka.encryption;

import com.google.common.base.Ticker;
import io.numaproj.kafka.encryption.aws.KmsDekGenerator;
import java.time.Duration;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds a {@link PayloadEncryptor} from producer properties, or returns {@code null} when encryption
 * is disabled. Presence of the AWS KMS key ARN is the enable switch; a malformed ARN fails fast at
 * startup.
 *
 * <p>The mirror of {@link EnvelopeDecryptionFactory}, and backend-agnostic in the same way: it reads
 * the config surface, owns the core DEK-reuse concern, and wires codec + rotation + generator.
 * AWS-specific knowledge (ARN validity, region, client lifecycle) lives in {@link KmsDekGenerator}.
 */
@Slf4j
public final class EnvelopeEncryptionFactory {

  // Default DEK reuse window. Matches the consumer-side cache TTL default: long enough to keep
  // GenerateDataKey calls rare, short enough to bound how many messages share one key. Operators can
  // override via EncryptionProps.DEK_TTL_MS.
  static final long DEFAULT_TTL_MS = Duration.ofHours(1).toMillis();

  private EnvelopeEncryptionFactory() {}

  /**
   * @return an encryptor when the key ARN is set, otherwise {@code null} (encryption disabled)
   * @throws IllegalArgumentException if the key ARN is set but malformed, or the TTL is not a positive
   *     long (fail-fast at startup)
   */
  public static PayloadEncryptor fromProps(Properties props) {
    String keyArn = props.getProperty(EncryptionProps.KEY_ARN);
    if (keyArn == null || keyArn.isBlank()) {
      return null;
    }
    // Parse the TTL before creating any AWS clients, so a bad TTL fails without allocating them.
    long ttlMillis = parseTtl(props.getProperty(EncryptionProps.DEK_TTL_MS));
    String assumeRoleArn = props.getProperty(EncryptionProps.ASSUME_ROLE_ARN);
    KmsDekGenerator kmsGenerator = KmsDekGenerator.create(keyArn.trim(), assumeRoleArn);
    log.info("Payload envelope encryption enabled (aws-kms)");
    // DEK reuse is backend-agnostic: wrap the KMS generator with a rotating decorator.
    DekGenerator generator =
        new RotatingDekGenerator(kmsGenerator, ttlMillis, Ticker.systemTicker());
    return new PayloadEncryptor(new JsonEnvelopeCodec(), generator);
  }

  private static long parseTtl(String value) {
    if (value == null || value.isBlank()) {
      return DEFAULT_TTL_MS;
    }
    try {
      long ttl = Long.parseLong(value.trim());
      if (ttl <= 0) {
        throw new IllegalArgumentException(
            EncryptionProps.DEK_TTL_MS + " must be a positive long: " + value);
      }
      return ttl;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          EncryptionProps.DEK_TTL_MS + " must be a long: " + value, e);
    }
  }
}
