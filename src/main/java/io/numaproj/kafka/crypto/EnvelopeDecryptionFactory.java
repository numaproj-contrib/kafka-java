package io.numaproj.kafka.crypto;

import java.time.Clock;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

/**
 * Builds a {@link PayloadDecryptor} from consumer properties, or returns {@code null} when
 * decryption is disabled. Presence of the AWS KMS key ARN is the enable switch (spec RD6); a
 * malformed ARN fails fast at startup (spec §8).
 */
@Slf4j
public final class EnvelopeDecryptionFactory {

  /** Presence enables decryption; must be a full KMS key ARN. */
  public static final String KEY_ARN =
      "payload.envelope.encryption.provider.aws-kms.key.arn";

  /** Plaintext-DEK cache TTL in milliseconds. */
  public static final String DEK_CACHE_TTL_MS =
      "payload.envelope.encryption.provider.aws-kms.dek.cache.ttl.ms";

  /** Existing key reused for KMS as well as Glue (spec RD3). */
  public static final String ASSUME_ROLE_ARN = "assumeRoleArn";

  static final long DEFAULT_TTL_MS = 3_600_000L;

  private static final String SESSION_NAME = "kafka-java-kms";

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
    keyArn = keyArn.trim();
    if (!isValidKmsKeyArn(keyArn)) {
      throw new IllegalArgumentException(
          "Invalid KMS key ARN for "
              + KEY_ARN
              + " (expected arn:aws:kms:<region>:<account>:key/<id>): "
              + keyArn);
    }
    long ttlMillis = parseTtl(props.getProperty(DEK_CACHE_TTL_MS));
    Region region = Region.of(Arn.fromString(keyArn).region().orElseThrow());
    String assumeRoleArn = props.getProperty(ASSUME_ROLE_ARN);
    KmsClient kmsClient = buildKmsClient(region, assumeRoleArn);
    log.info("Payload envelope decryption enabled (aws-kms, region {})", region.id());
    KeyProvider keyProvider =
        new AwsKmsKeyProvider(kmsClient, keyArn, new DekCache(ttlMillis, Clock.systemUTC()));
    return new PayloadDecryptor(new JsonEnvelopeCodec(), keyProvider);
  }

  static boolean isValidKmsKeyArn(String candidate) {
    try {
      Arn arn = Arn.fromString(candidate);
      return "kms".equals(arn.service())
          && arn.region().filter(r -> !r.isBlank()).isPresent()
          && arn.resourceAsString().startsWith("key/");
    } catch (RuntimeException e) {
      return false;
    }
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

  private static KmsClient buildKmsClient(Region region, String assumeRoleArn) {
    // Pin the sync HTTP client explicitly: the AWS SDK pulls in more than one sync implementation
    // (apache-client, url-connection-client), and leaving it unset throws "Multiple HTTP
    // implementations were found on the classpath".
    var builder = KmsClient.builder().region(region).httpClient(UrlConnectionHttpClient.create());
    if (assumeRoleArn != null && !assumeRoleArn.isBlank()) {
      StsClient stsClient =
          StsClient.builder().region(region).httpClient(UrlConnectionHttpClient.create()).build();
      AwsCredentialsProvider credentials =
          StsAssumeRoleCredentialsProvider.builder()
              .stsClient(stsClient)
              .refreshRequest(
                  AssumeRoleRequest.builder()
                      .roleArn(assumeRoleArn.trim())
                      .roleSessionName(SESSION_NAME)
                      .build())
              .build();
      builder.credentialsProvider(credentials);
    }
    // Otherwise the KMS client uses the default credentials provider chain (IRSA / env / etc.).
    return builder.build();
  }
}
