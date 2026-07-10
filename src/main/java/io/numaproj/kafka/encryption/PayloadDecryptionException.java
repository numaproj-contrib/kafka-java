package io.numaproj.kafka.encryption;

/**
 * Unrecoverable error while decrypting an envelope-encrypted Kafka value. Thrown by the codec, DEK
 * unwrapper, and decryptor; surfaces as a deserialization failure and drives the source's fail-fast
 * behavior.
 *
 * <p>Messages must never contain the plaintext DEK or decrypted payload.
 */
public class PayloadDecryptionException extends RuntimeException {

  public PayloadDecryptionException(String message) {
    super(message);
  }

  public PayloadDecryptionException(String message, Throwable cause) {
    super(message, cause);
  }
}
