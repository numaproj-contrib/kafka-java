package io.numaproj.kafka.crypto;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;

/**
 * {@link KeyProvider} backed by AWS KMS. Unwraps the DEK via {@code kms:Decrypt}, passing the
 * configured key ARN as {@code KeyId} so KMS rejects any ciphertext not wrapped under the expected
 * key (spec §7.2 P1). Recovered DEKs are cached (spec §7.2 P2).
 *
 * <p>The plaintext DEK is never logged (spec SR1).
 */
@Slf4j
public class AwsKmsKeyProvider implements KeyProvider {

  private final KmsClient kmsClient;
  private final String keyArn;
  private final DekCache dekCache;

  public AwsKmsKeyProvider(KmsClient kmsClient, String keyArn, DekCache dekCache) {
    this.kmsClient = kmsClient;
    this.keyArn = keyArn;
    this.dekCache = dekCache;
  }

  @Override
  public byte[] unwrap(byte[] wrappedDek) {
    byte[] cached = dekCache.get(wrappedDek);
    if (cached != null) {
      return cached;
    }
    DecryptResponse response =
        kmsClient.decrypt(
            DecryptRequest.builder()
                .keyId(keyArn)
                .ciphertextBlob(SdkBytes.fromByteArray(wrappedDek))
                .build());
    byte[] dek = response.plaintext().asByteArray();
    dekCache.put(wrappedDek, dek);
    return dek;
  }

  @Override
  public void close() {
    kmsClient.close();
  }
}
