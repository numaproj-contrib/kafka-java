package io.numaproj.kafka.encryption;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Orchestrates encryption: {@code generator.generate → AEAD encrypt → codec.serialize}. The inverse
 * of {@link PayloadDecryptor}; the only supported algorithm is {@code AES-256-GCM}.
 *
 * <p>A fresh 12-byte nonce is drawn for every message from a single long-lived {@link SecureRandom}.
 * This is the producer-side obligation AES-GCM depends on: reusing a nonce under one DEK exposes the
 * XOR of the affected plaintexts and can let an attacker forge authentication tags. Nonces are never
 * derived from message content or a counter that could restart.
 *
 * <p>Neither the plaintext DEK nor the plaintext payload is logged.
 */
public class PayloadEncryptor {

  static final String ALG = "AES-256-GCM";
  static final int ENVELOPE_VERSION = 1;
  private static final String AES_GCM_TRANSFORMATION = "AES/GCM/NoPadding";
  private static final int GCM_TAG_BITS = 128;
  private static final int NONCE_BYTES = 12;

  private final EnvelopeCodec codec;
  private final DekGenerator generator;
  private final SecureRandom random = new SecureRandom();

  public PayloadEncryptor(EnvelopeCodec codec, DekGenerator generator) {
    this.codec = codec;
    this.generator = generator;
  }

  /**
   * Encrypt already-serialized payload bytes into the envelope that goes on the wire. To this layer
   * the payload is opaque: whatever the value serializer produced (a Glue frame, Avro, JSON, raw
   * bytes) is encrypted as-is, which is why encryption is always the final step before producing.
   *
   * @throws PayloadEncryptionException if the AEAD cipher rejects its inputs or the envelope cannot
   *     be written
   */
  public byte[] encrypt(byte[] payload) {
    Dek dek = generator.generate();
    byte[] nonce = new byte[NONCE_BYTES];
    random.nextBytes(nonce);
    byte[] ciphertext;
    try {
      Cipher cipher = Cipher.getInstance(AES_GCM_TRANSFORMATION);
      cipher.init(
          Cipher.ENCRYPT_MODE,
          new SecretKeySpec(dek.plaintext(), "AES"),
          new GCMParameterSpec(GCM_TAG_BITS, nonce));
      // JCE appends the 16-byte authentication tag to the ciphertext, which is the wire layout.
      ciphertext = cipher.doFinal(payload);
    } catch (GeneralSecurityException e) {
      // Do not include the plaintext payload or the DEK in the message.
      throw new PayloadEncryptionException("AEAD encryption failed", e);
    }
    return codec.serialize(new Envelope(ENVELOPE_VERSION, ALG, dek.wrapped(), nonce, ciphertext));
  }

  public void close() {
    generator.close();
  }
}
