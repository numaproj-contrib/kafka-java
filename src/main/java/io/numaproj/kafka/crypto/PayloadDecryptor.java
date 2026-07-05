package io.numaproj.kafka.crypto;

import java.security.GeneralSecurityException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Orchestrates decryption: {@code codec.parse → keyProvider.unwrap → AEAD decrypt} (spec §6.3). The
 * AEAD algorithm is selected by the {@code alg} the codec reports; the only supported value is
 * {@code AES-256-GCM}.
 *
 * <p>Neither the plaintext DEK nor the decrypted payload is logged (spec SR1/SR2).
 */
public class PayloadDecryptor {

  static final String SUPPORTED_ALG = "AES-256-GCM";
  private static final String AES_GCM_TRANSFORMATION = "AES/GCM/NoPadding";
  private static final int GCM_TAG_BITS = 128;

  private final EnvelopeCodec codec;
  private final KeyProvider keyProvider;

  public PayloadDecryptor(EnvelopeCodec codec, KeyProvider keyProvider) {
    this.codec = codec;
    this.keyProvider = keyProvider;
  }

  /**
   * Decrypt a Kafka value into its plaintext bytes (which the delegate deserializer then decodes).
   *
   * @throws PayloadDecryptionException on any unrecoverable error (malformed envelope, unsupported
   *     algorithm, key-unwrap failure, or AEAD authentication failure)
   */
  public byte[] decrypt(String topic, byte[] value) {
    Envelope envelope = codec.parse(topic, value);
    if (!SUPPORTED_ALG.equals(envelope.alg())) {
      throw new PayloadDecryptionException("Unsupported alg: " + envelope.alg());
    }
    byte[] dek = keyProvider.unwrap(envelope.wrappedDek());
    try {
      Cipher cipher = Cipher.getInstance(AES_GCM_TRANSFORMATION);
      cipher.init(
          Cipher.DECRYPT_MODE,
          new SecretKeySpec(dek, "AES"),
          new GCMParameterSpec(GCM_TAG_BITS, envelope.nonce()));
      return cipher.doFinal(envelope.ciphertext());
    } catch (GeneralSecurityException e) {
      // Includes AEADBadTagException (tampering / wrong key). Do not include plaintext in the message.
      throw new PayloadDecryptionException("AEAD decryption failed", e);
    }
  }

  public void close() {
    keyProvider.close();
  }
}
