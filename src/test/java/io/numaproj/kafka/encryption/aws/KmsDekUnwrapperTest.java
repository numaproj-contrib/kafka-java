package io.numaproj.kafka.encryption.aws;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.numaproj.kafka.common.aws.AwsCredentials;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.KmsException;

@ExtendWith(MockitoExtension.class)
class KmsDekUnwrapperTest {

  private static final String KEY_ARN = "arn:aws:kms:us-east-1:123456789012:key/abcd-1234";
  private static final byte[] WRAPPED = {1, 2, 3, 4};
  private static final byte[] DEK = new byte[32];

  @Mock private KmsClient kms;
  @Mock private AwsCredentials credentials;

  private KmsDekUnwrapper unwrapper() {
    return new KmsDekUnwrapper(kms, credentials, KEY_ARN);
  }

  @Test
  void unwrapsAndPassesConfiguredKeyId() {
    when(kms.decrypt(any(DecryptRequest.class)))
        .thenReturn(DecryptResponse.builder().plaintext(SdkBytes.fromByteArray(DEK)).build());

    assertArrayEquals(DEK, unwrapper().unwrap(WRAPPED));

    ArgumentCaptor<DecryptRequest> captor = ArgumentCaptor.forClass(DecryptRequest.class);
    verify(kms).decrypt(captor.capture());
    assertEquals(KEY_ARN, captor.getValue().keyId());
    assertArrayEquals(WRAPPED, captor.getValue().ciphertextBlob().asByteArray());
  }

  @Test
  void propagatesKmsFailure() {
    when(kms.decrypt(any(DecryptRequest.class)))
        .thenThrow(KmsException.builder().message("access denied").build());

    assertThrows(KmsException.class, () -> unwrapper().unwrap(WRAPPED));
  }

  @Test
  void closeReleasesCredentialsThenKmsClient() {
    unwrapper().close();

    // credentials subtree (STS client + provider) first, then the KMS client.
    InOrder inOrder = inOrder(credentials, kms);
    inOrder.verify(credentials).close();
    inOrder.verify(kms).close();
  }

  @Test
  void createRejectsMalformedArn() {
    assertThrows(IllegalArgumentException.class, () -> KmsDekUnwrapper.create("not-an-arn", null));
  }

}
