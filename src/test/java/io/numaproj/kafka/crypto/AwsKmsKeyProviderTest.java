package io.numaproj.kafka.crypto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.KmsException;

@ExtendWith(MockitoExtension.class)
class AwsKmsKeyProviderTest {

  private static final String KEY_ARN = "arn:aws:kms:us-east-1:123456789012:key/abcd-1234";
  private static final byte[] WRAPPED = {1, 2, 3, 4};
  private static final byte[] DEK = new byte[32];

  @Mock private KmsClient kmsClient;

  private DecryptResponse dekResponse() {
    return DecryptResponse.builder().plaintext(SdkBytes.fromByteArray(DEK)).build();
  }

  @Test
  void unwrapsAndPassesConfiguredKeyIdAndCaches() {
    when(kmsClient.decrypt(any(DecryptRequest.class))).thenReturn(dekResponse());
    AdjustableClock clock = new AdjustableClock(0);
    AwsKmsKeyProvider provider =
        new AwsKmsKeyProvider(kmsClient, KEY_ARN, new DekCache(1000, clock));

    assertArrayEquals(DEK, provider.unwrap(WRAPPED));

    ArgumentCaptor<DecryptRequest> captor = ArgumentCaptor.forClass(DecryptRequest.class);
    verify(kmsClient).decrypt(captor.capture());
    assertEquals(KEY_ARN, captor.getValue().keyId());
    assertArrayEquals(WRAPPED, captor.getValue().ciphertextBlob().asByteArray());

    // Second call with the same wrapped DEK is served from cache — no extra KMS call.
    assertArrayEquals(DEK, provider.unwrap(WRAPPED));
    verify(kmsClient, times(1)).decrypt(any(DecryptRequest.class));
  }

  @Test
  void refetchesAfterTtlExpiry() {
    when(kmsClient.decrypt(any(DecryptRequest.class))).thenReturn(dekResponse());
    AdjustableClock clock = new AdjustableClock(0);
    AwsKmsKeyProvider provider =
        new AwsKmsKeyProvider(kmsClient, KEY_ARN, new DekCache(1000, clock));

    provider.unwrap(WRAPPED);
    clock.advance(1001); // past TTL
    provider.unwrap(WRAPPED);

    verify(kmsClient, times(2)).decrypt(any(DecryptRequest.class));
  }

  @Test
  void propagatesKmsFailure() {
    when(kmsClient.decrypt(any(DecryptRequest.class)))
        .thenThrow(KmsException.builder().message("access denied").build());
    AwsKmsKeyProvider provider =
        new AwsKmsKeyProvider(kmsClient, KEY_ARN, new DekCache(1000, new AdjustableClock(0)));

    assertThrows(KmsException.class, () -> provider.unwrap(WRAPPED));
  }
}
