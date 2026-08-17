package io.numaproj.kafka.encryption;

/**
 * Thrown when a payload cannot be encrypted: DEK generation failed, the envelope could not be
 * written, or the AEAD cipher rejected its inputs.
 *
 * <p>Never carries the plaintext payload or key material in its message.
 */
public class PayloadEncryptionException extends RuntimeException {

  public PayloadEncryptionException(String message) {
    super(message);
  }

  public PayloadEncryptionException(String message, Throwable cause) {
    super(message, cause);
  }
}
