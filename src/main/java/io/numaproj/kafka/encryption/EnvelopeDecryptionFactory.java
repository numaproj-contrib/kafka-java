package io.numaproj.kafka.encryption;

import io.numaproj.kafka.encryption.aws.KmsDekUnwrapper;
import java.time.Duration;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds a {@link PayloadDecryptor} from consumer properties, or returns {@code null} when
 * decryption is disabled. Presence of the AWS KMS key ARN is the enable switch; a malformed ARN
 * fails fast at startup.
 *
 * <p>This factory is backend-agnostic orchestration: it reads the config surface, owns the core
 * DEK-cache concern, and wires codec + caching + unwrapper. AWS-specific knowledge (ARN validity,
 * region, client lifecycle) lives in {@link KmsDekUnwrapper}.
 */
@Slf4j
public final class EnvelopeDecryptionFactory {

  /** Presence enables decryption; must be a full KMS key ARN. */
  public static final String KEY_ARN = "payload.envelope.encryption.provider.aws-kms.key.arn";

  /** Plaintext-DEK cache TTL in milliseconds. */
  public static final String DEK_CACHE_TTL_MS = "payload.envelope.encryption.dek.cache.ttl.ms";

  /** Existing key reused for KMS as well as Glue. */
  public static final String ASSUME_ROLE_ARN = "assumeRoleArn";

  // Default DEK cache TTL. One hour trades off KMS unwrap cost/latency (a cache hit avoids a
  // Decrypt call per message) against how long a plaintext DEK lives in memory: long enough to keep
  // hit rates high given infrequent DEK rotation, short enough to bound staleness. Operators can
  // override via DEK_CACHE_TTL_MS.
  static final long DEFAULT_TTL_MS = Duration.ofHours(1).toMillis();

  private EnvelopeDecryptionFactory() {}

  /**
   * @return a decryptor when the key ARN is set, otherwise {@code null} (decryption disabled)
   * @throws IllegalArgumentException if the key ARN is set but malformed, or the TTL is not a
   *     positive long (fail-fast at startup)
   */
  public static PayloadDecryptor fromProps(Properties props) {
    String keyArn = props.getProperty(KEY_ARN);
    if (keyArn == null || keyArn.isBlank()) {
      return null;
    }
    // Parse the TTL before creating any AWS clients, so a bad TTL fails without allocating them.
    long ttlMillis = parseTtl(props.getProperty(DEK_CACHE_TTL_MS));
    String assumeRoleArn = props.getProperty(ASSUME_ROLE_ARN);
    KmsDekUnwrapper kmsUnwrapper = KmsDekUnwrapper.create(keyArn.trim(), assumeRoleArn);
    log.info("Payload envelope decryption enabled (aws-kms)");
    // Caching is backend-agnostic: wrap the KMS unwrapper with a DEK cache decorator.
    DekUnwrapper unwrapper = new CachingDekUnwrapper(kmsUnwrapper, new DekCache(ttlMillis));
    return new PayloadDecryptor(new JsonEnvelopeCodec(), unwrapper);
  }

  private static long parseTtl(String value) {
    if (value == null || value.isBlank()) {
      return DEFAULT_TTL_MS;
    }
    try {
      long ttl = Long.parseLong(value.trim());
      if (ttl <= 0) {
        throw new IllegalArgumentException(DEK_CACHE_TTL_MS + " must be a positive long: " + value);
      }
      return ttl;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(DEK_CACHE_TTL_MS + " must be a long: " + value, e);
    }
  }
}
