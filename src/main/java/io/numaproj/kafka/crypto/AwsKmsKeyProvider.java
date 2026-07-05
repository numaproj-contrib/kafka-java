package io.numaproj.kafka.crypto;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;

/**
 * {@link KeyProvider} backed by AWS KMS. Unwraps the DEK via {@code kms:Decrypt}, passing the
 * configured key ARN as {@code KeyId} so KMS rejects any ciphertext not wrapped under the expected
 * key (spec §7.2 P1).
 *
 * <p>This provider does no caching — DEK caching is provider-agnostic and applied by
 * {@link CachingKeyProvider}. The plaintext DEK is never logged (spec SR1).
 */
public class AwsKmsKeyProvider implements KeyProvider {

  private final KmsClient kmsClient;
  private final String keyArn;

  public AwsKmsKeyProvider(KmsClient kmsClient, String keyArn) {
    this.kmsClient = kmsClient;
    this.keyArn = keyArn;
  }

  @Override
  public byte[] unwrap(byte[] wrappedDek) {
    DecryptResponse response =
        kmsClient.decrypt(
            DecryptRequest.builder()
                .keyId(keyArn)
                .ciphertextBlob(SdkBytes.fromByteArray(wrappedDek))
                .build());
    return response.plaintext().asByteArray();
  }

  @Override
  public void close() {
    kmsClient.close();
  }
}
