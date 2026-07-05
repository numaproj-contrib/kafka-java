package io.numaproj.kafka.crypto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DecryptingDeserializerTest {

  @Mock private Deserializer<String> delegate;
  @Mock private PayloadDecryptor decryptor;

  @Test
  void decryptsThenDelegates() {
    byte[] data = {1, 2, 3};
    byte[] plaintext = {4, 5, 6};
    when(decryptor.decrypt("t", data)).thenReturn(plaintext);
    when(delegate.deserialize("t", plaintext)).thenReturn("decoded");

    DecryptingDeserializer<String> d = new DecryptingDeserializer<>(delegate, decryptor);

    assertEquals("decoded", d.deserialize("t", data));
    verify(delegate).deserialize("t", plaintext);
  }

  @Test
  void passesThroughNullWithoutDecrypting() {
    DecryptingDeserializer<String> d = new DecryptingDeserializer<>(delegate, decryptor);

    d.deserialize("t", (byte[]) null);

    verify(delegate).deserialize(eq("t"), (byte[]) any());
    verifyNoInteractions(decryptor);
  }

  @Test
  void propagatesDecryptFailure() {
    when(decryptor.decrypt(any(), any())).thenThrow(new PayloadDecryptionException("boom"));
    DecryptingDeserializer<String> d = new DecryptingDeserializer<>(delegate, decryptor);

    assertThrows(PayloadDecryptionException.class, () -> d.deserialize("t", new byte[] {1}));
    verify(delegate, never()).deserialize(any(), any(byte[].class));
  }
}
