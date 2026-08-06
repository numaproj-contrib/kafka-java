package io.numaproj.kafka.encryption;

/**
 * The payload-envelope-encryption configuration surface, shared by the source and the sink so a
 * producer and a consumer of the same topic are configured with the same key names.
 *
 * <p>These keys are consumed by kafka-java itself and stripped before the properties reach a Kafka
 * client.
 */
public final class EncryptionProps {

  /** Prefix for every kafka-java-managed envelope-encryption key. */
  public static final String PREFIX = "payload.envelope.encryption.";

  /** Presence enables encryption/decryption; must be a full KMS key ARN. */
  public static final String KEY_ARN = PREFIX + "provider.aws-kms.key.arn";

  /** Source: how long a recovered plaintext DEK is cached. */
  public static final String DEK_CACHE_TTL_MS = PREFIX + "dek.cache.ttl.ms";

  /** Existing key reused for KMS as well as Glue. */
  public static final String ASSUME_ROLE_ARN = "assumeRoleArn";

  private EncryptionProps() {}
}
