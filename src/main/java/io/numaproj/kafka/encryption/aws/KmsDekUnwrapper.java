package io.numaproj.kafka.encryption.aws;

import io.numaproj.kafka.common.aws.AwsCredentials;
import io.numaproj.kafka.encryption.DekUnwrapper;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;

/**
 * {@link DekUnwrapper} backed by AWS KMS. Unwraps the DEK via {@code kms:Decrypt} with the
 * configured key ARN pinned as {@code KeyId}, so KMS rejects any ciphertext not wrapped under the
 * expected key.
 *
 * <p>Owns the KMS client and its {@link AwsCredentials}; {@link #close()} releases both. The
 * credentials are a separate concern ({@code assumeRoleArn} vs. the SDK default chain) delegated to
 * {@link AwsCredentials}. Does no caching — that is backend-agnostic and applied by the core
 * {@code CachingDekUnwrapper}. The plaintext DEK is never logged.
 */
@Slf4j
public class KmsDekUnwrapper implements DekUnwrapper {

  private final KmsClient kms;
  private final AwsCredentials credentials;
  private final String keyArn;

  KmsDekUnwrapper(KmsClient kms, AwsCredentials credentials, String keyArn) {
    this.kms = kms;
    this.credentials = credentials;
    this.keyArn = keyArn;
  }

  /**
   * Validates the KMS key ARN, derives the region from it, and builds the owned client stack (KMS
   * client + credentials). If the KMS client fails to build, the credentials subtree is closed
   * before propagating so nothing leaks.
   *
   * @throws IllegalArgumentException if the ARN is not a well-formed KMS key ARN (fail-fast at
   *     startup)
   */
  public static KmsDekUnwrapper create(String keyArn, String assumeRoleArn) {
    Region region = KmsKeys.validateAndGetRegion(keyArn);
    AwsCredentials credentials = AwsCredentials.resolve(region, assumeRoleArn);
    try {
      // Pin the sync HTTP client explicitly (the AWS SDK errors when it finds more than one on the
      // classpath — apache-client + url-connection-client are both present).
      var builder = KmsClient.builder().region(region).httpClient(UrlConnectionHttpClient.create());
      if (credentials.credentials() != null) {
        builder.credentialsProvider(credentials.credentials());
      }
      log.info("Initializing aws-kms DEK unwrapper (region {})", region.id());
      return new KmsDekUnwrapper(builder.build(), credentials, keyArn);
    } catch (RuntimeException e) {
      credentials.close();
      throw e;
    }
  }

  @Override
  public byte[] unwrap(byte[] wrappedDek) {
    DecryptResponse response =
        this.kms.decrypt(
            DecryptRequest.builder()
                .keyId(this.keyArn)
                .ciphertextBlob(SdkBytes.fromByteArray(wrappedDek))
                .build());
    return response.plaintext().asByteArray();
  }

  @Override
  public void close() {
    closeQuietly(this.credentials);
    closeQuietly(this.kms);
  }

  private static void closeQuietly(AutoCloseable resource) {
    if (resource == null) {
      return;
    }
    try {
      resource.close();
    } catch (Exception e) {
      log.warn("Failed to close {} while releasing the KMS unwrapper", resource.getClass(), e);
    }
  }
}
