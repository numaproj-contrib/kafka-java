package io.numaproj.kafka.crypto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;

class PayloadDecryptorTest {

  private static final byte[] WRAPPED_DEK = {7, 7, 7};

  private static byte[] randomBytes(int n) {
    byte[] b = new byte[n];
    new SecureRandom().nextBytes(b);
    return b;
  }

  /** AES-256-GCM encrypt (tag appended), mirroring what a producer would emit. */
  private static byte[] encrypt(byte[] dek, byte[] nonce, byte[] plaintext) throws Exception {
    Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
    cipher.init(
        Cipher.ENCRYPT_MODE, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(128, nonce));
    return cipher.doFinal(plaintext);
  }

  @Test
  void decryptsRoundTrip() throws Exception {
    byte[] dek = randomBytes(32);
    byte[] nonce = randomBytes(12);
    byte[] plaintext = "hello world".getBytes(StandardCharsets.UTF_8);
    byte[] ciphertext = encrypt(dek, nonce, plaintext);

    EnvelopeCodec codec = (topic, value) -> new Envelope(1, "AES-256-GCM", WRAPPED_DEK, nonce, ciphertext);
    KeyProvider keyProvider = mock(KeyProvider.class);
    when(keyProvider.unwrap(any())).thenReturn(dek);

    PayloadDecryptor decryptor = new PayloadDecryptor(codec, keyProvider);

    assertArrayEquals(plaintext, decryptor.decrypt("t", new byte[] {0}));
  }

  @Test
  void rejectsUnsupportedAlgorithmBeforeUnwrapping() {
    EnvelopeCodec codec =
        (topic, value) -> new Envelope(1, "AES-128-GCM", WRAPPED_DEK, new byte[12], new byte[16]);
    KeyProvider keyProvider = mock(KeyProvider.class);

    PayloadDecryptor decryptor = new PayloadDecryptor(codec, keyProvider);

    assertThrows(PayloadDecryptionException.class, () -> decryptor.decrypt("t", new byte[] {0}));
    verifyNoInteractions(keyProvider);
  }

  @Test
  void failsOnTamperedCiphertext() throws Exception {
    byte[] dek = randomBytes(32);
    byte[] nonce = randomBytes(12);
    byte[] ciphertext = encrypt(dek, nonce, "secret".getBytes(StandardCharsets.UTF_8));
    ciphertext[0] ^= 0x01; // tamper

    EnvelopeCodec codec = (topic, value) -> new Envelope(1, "AES-256-GCM", WRAPPED_DEK, nonce, ciphertext);
    KeyProvider keyProvider = mock(KeyProvider.class);
    when(keyProvider.unwrap(any())).thenReturn(dek);

    PayloadDecryptor decryptor = new PayloadDecryptor(codec, keyProvider);

    PayloadDecryptionException ex =
        assertThrows(PayloadDecryptionException.class, () -> decryptor.decrypt("t", new byte[] {0}));
    assertInstanceOf(AEADBadTagException.class, ex.getCause());
  }
}
