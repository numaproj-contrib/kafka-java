package io.numaproj.kafka.encryption.aws;

import io.numaproj.kafka.common.aws.AwsCredentials;
import io.numaproj.kafka.encryption.Dek;
import io.numaproj.kafka.encryption.DekGenerator;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;

/**
 * {@link DekGenerator} backed by AWS KMS. Generates a 256-bit DEK via {@code kms:GenerateDataKey}
 * under the configured key, and returns both the plaintext key and the ciphertext blob KMS wrapped it
 * into.
 *
 * <p>Owns the KMS client and its {@link AwsCredentials}; {@link #close()} releases both. Does no key
 * reuse — that is backend-agnostic and applied by the core {@code ProcessLifetimeDekGenerator}. The plaintext
 * DEK is never logged.
 */
@Slf4j
public class KmsDekGenerator implements DekGenerator {

  private final KmsClient kms;
  private final AwsCredentials credentials;
  private final String keyArn;

  KmsDekGenerator(KmsClient kms, AwsCredentials credentials, String keyArn) {
    this.kms = kms;
    this.credentials = credentials;
    this.keyArn = keyArn;
  }

  /**
   * Validates the KMS key ARN, derives the region from it, and builds the owned client stack (KMS
   * client + credentials). If the KMS client fails to build, the credentials subtree is closed before
   * propagating so nothing leaks.
   *
   * @throws IllegalArgumentException if the ARN is not a well-formed KMS key ARN (fail-fast at
   *     startup); a bare alias is rejected, since the region is derived from the key itself
   */
  public static KmsDekGenerator create(String keyArn, String assumeRoleArn) {
    Region region = KmsKeys.validateAndGetRegion(keyArn);
    AwsCredentials credentials = AwsCredentials.resolve(region, assumeRoleArn);
    try {
      // Pin the sync HTTP client explicitly (the AWS SDK errors when it finds more than one on the
      // classpath — apache-client + url-connection-client are both present).
      var builder = KmsClient.builder().region(region).httpClient(UrlConnectionHttpClient.create());
      if (credentials.credentials() != null) {
        builder.credentialsProvider(credentials.credentials());
      }
      log.info("Initializing aws-kms DEK generator (region {})", region.id());
      return new KmsDekGenerator(builder.build(), credentials, keyArn);
    } catch (RuntimeException e) {
      credentials.close();
      throw e;
    }
  }

  @Override
  public Dek generate() {
    GenerateDataKeyResponse response =
        this.kms.generateDataKey(
            GenerateDataKeyRequest.builder()
                .keyId(this.keyArn)
                .keySpec(DataKeySpec.AES_256)
                .build());
    return new Dek(response.plaintext().asByteArray(), response.ciphertextBlob().asByteArray());
  }

  @Override
  public void close() {
    AwsCredentials.closeQuietly(this.credentials, "the KMS generator");
    AwsCredentials.closeQuietly(this.kms, "the KMS generator");
  }
}
