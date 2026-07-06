package io.numaproj.kafka.encryption;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;

/**
 * {@link DekUnwrapper} backed by AWS KMS. Unwraps the DEK via {@code kms:Decrypt}, passing the
 * configured key ARN as {@code KeyId} so KMS rejects any ciphertext not wrapped under the expected
 * key.
 *
 * <p>This unwrapper does no caching — DEK caching is backend-agnostic and applied by
 * {@link CachingDekUnwrapper}. The plaintext DEK is never logged.
 */
public class AwsKmsDekUnwrapper implements DekUnwrapper {

  private final KmsClient kmsClient;
  private final String keyArn;

  public AwsKmsDekUnwrapper(KmsClient kmsClient, String keyArn) {
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
