package io.numaproj.kafka.encryption.aws;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.numaproj.kafka.common.aws.AwsCredentials;
import io.numaproj.kafka.encryption.Dek;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.kms.model.KmsException;

@ExtendWith(MockitoExtension.class)
class KmsDekGeneratorTest {

  private static final String KEY_ARN = "arn:aws:kms:us-east-1:123456789012:key/abcd-1234";
  private static final byte[] PLAINTEXT_DEK = new byte[32];
  private static final byte[] WRAPPED_DEK = {1, 2, 3, 4};

  @Mock private KmsClient kms;
  @Mock private AwsCredentials credentials;

  private KmsDekGenerator generator() {
    return new KmsDekGenerator(kms, credentials, KEY_ARN);
  }

  @Test
  void generatesAes256DekUnderTheConfiguredKey() {
    when(kms.generateDataKey(any(GenerateDataKeyRequest.class)))
        .thenReturn(
            GenerateDataKeyResponse.builder()
                .plaintext(SdkBytes.fromByteArray(PLAINTEXT_DEK))
                .ciphertextBlob(SdkBytes.fromByteArray(WRAPPED_DEK))
                .build());

    Dek dek = generator().generate();

    assertArrayEquals(PLAINTEXT_DEK, dek.plaintext());
    assertArrayEquals(WRAPPED_DEK, dek.wrapped());

    ArgumentCaptor<GenerateDataKeyRequest> captor =
        ArgumentCaptor.forClass(GenerateDataKeyRequest.class);
    verify(kms).generateDataKey(captor.capture());
    assertEquals(KEY_ARN, captor.getValue().keyId());
    assertEquals(DataKeySpec.AES_256, captor.getValue().keySpec());
  }

  @Test
  void propagatesKmsFailure() {
    when(kms.generateDataKey(any(GenerateDataKeyRequest.class)))
        .thenThrow(KmsException.builder().message("access denied").build());

    assertThrows(KmsException.class, () -> generator().generate());
  }

  @Test
  void closeReleasesCredentialsThenKmsClient() {
    generator().close();

    InOrder inOrder = inOrder(credentials, kms);
    inOrder.verify(credentials).close();
    inOrder.verify(kms).close();
  }

  @Test
  void createRejectsMalformedArn() {
    assertThrows(IllegalArgumentException.class, () -> KmsDekGenerator.create("not-an-arn", null));
  }

  @Test
  void createRejectsBareAlias() {
    // The contract's alias form carries no region, so a full ARN is required.
    assertThrows(
        IllegalArgumentException.class,
        () -> KmsDekGenerator.create("alias/my-key", null));
  }
}
