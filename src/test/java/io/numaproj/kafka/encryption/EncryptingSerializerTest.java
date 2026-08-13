package io.numaproj.kafka.encryption;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class EncryptingSerializerTest {

  private static final String TOPIC = "t";
  private static final byte[] SERIALIZED = {10, 20, 30};
  private static final byte[] ENCRYPTED = {99};

  @SuppressWarnings("unchecked")
  private final Serializer<String> delegate = mock(Serializer.class);

  private final PayloadEncryptor encryptor = mock(PayloadEncryptor.class);

  private EncryptingSerializer<String> underTest() {
    return new EncryptingSerializer<>(delegate, encryptor);
  }

  @Test
  void serializesThenEncrypts() {
    when(delegate.serialize(TOPIC, "record")).thenReturn(SERIALIZED);
    when(encryptor.encrypt(SERIALIZED)).thenReturn(ENCRYPTED);

    assertArrayEquals(ENCRYPTED, underTest().serialize(TOPIC, "record"));

    // Encryption must be the final step: the delegate runs first, on the plaintext record.
    InOrder inOrder = inOrder(delegate, encryptor);
    inOrder.verify(delegate).serialize(TOPIC, "record");
    inOrder.verify(encryptor).encrypt(SERIALIZED);
  }

  /**
   * A tombstone only marks a key for deletion while it stays null on the wire, so an empty
   * serialized value is produced as-is. The source-side mirror is {@code
   * DecryptingDeserializerTest#passesThroughNullWithoutDecrypting}.
   */
  @Test
  void passesThroughNullWithoutEncrypting() {
    when(delegate.serialize(TOPIC, null)).thenReturn(null);

    assertNull(underTest().serialize(TOPIC, null));

    verifyNoInteractions(encryptor);
  }

  @Test
  void passesThroughEmptyWithoutEncrypting() {
    when(delegate.serialize(TOPIC, "record")).thenReturn(new byte[0]);

    assertArrayEquals(new byte[0], underTest().serialize(TOPIC, "record"));

    verifyNoInteractions(encryptor);
  }

  @Test
  void propagatesEncryptionFailure() {
    when(delegate.serialize(TOPIC, "record")).thenReturn(SERIALIZED);
    when(encryptor.encrypt(SERIALIZED))
        .thenThrow(new PayloadEncryptionException("AEAD encryption failed"));

    assertThrows(PayloadEncryptionException.class, () -> underTest().serialize(TOPIC, "record"));
  }

  @Test
  void closeClosesEncryptorThenDelegate() {
    underTest().close();

    InOrder inOrder = inOrder(encryptor, delegate);
    inOrder.verify(encryptor).close();
    inOrder.verify(delegate).close();
  }

  @Test
  void closeClosesDelegateEvenWhenEncryptorFails() {
    doThrow(new RuntimeException("boom")).when(encryptor).close();

    assertThrows(RuntimeException.class, () -> underTest().close());

    verify(delegate).close();
  }
}
