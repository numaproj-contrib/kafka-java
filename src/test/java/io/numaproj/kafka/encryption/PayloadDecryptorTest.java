package io.numaproj.kafka.encryption;

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

    EnvelopeCodec codec = value -> new Envelope(1, "AES-256-GCM", WRAPPED_DEK, nonce, ciphertext);
    DekUnwrapper unwrapper = mock(DekUnwrapper.class);
    when(unwrapper.unwrap(any())).thenReturn(dek);

    PayloadDecryptor decryptor = new PayloadDecryptor(codec, unwrapper);

    assertArrayEquals(plaintext, decryptor.decrypt(new byte[] {0}));
  }

  @Test
  void rejectsUnsupportedAlgorithmBeforeUnwrapping() {
    EnvelopeCodec codec =
        value -> new Envelope(1, "AES-128-GCM", WRAPPED_DEK, new byte[12], new byte[16]);
    DekUnwrapper unwrapper = mock(DekUnwrapper.class);

    PayloadDecryptor decryptor = new PayloadDecryptor(codec, unwrapper);

    assertThrows(PayloadDecryptionException.class, () -> decryptor.decrypt(new byte[] {0}));
    verifyNoInteractions(unwrapper);
  }

  @Test
  void failsOnTamperedCiphertext() throws Exception {
    byte[] dek = randomBytes(32);
    byte[] nonce = randomBytes(12);
    byte[] ciphertext = encrypt(dek, nonce, "secret".getBytes(StandardCharsets.UTF_8));
    ciphertext[0] ^= 0x01; // tamper

    EnvelopeCodec codec = value -> new Envelope(1, "AES-256-GCM", WRAPPED_DEK, nonce, ciphertext);
    DekUnwrapper unwrapper = mock(DekUnwrapper.class);
    when(unwrapper.unwrap(any())).thenReturn(dek);

    PayloadDecryptor decryptor = new PayloadDecryptor(codec, unwrapper);

    PayloadDecryptionException ex =
        assertThrows(PayloadDecryptionException.class, () -> decryptor.decrypt(new byte[] {0}));
    assertInstanceOf(AEADBadTagException.class, ex.getCause());
  }
}
