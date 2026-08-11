package io.numaproj.kafka.encryption;

import io.numaproj.kafka.common.BadRecordException;

/**
 * Unrecoverable error while decrypting an envelope-encrypted Kafka value. Thrown by the codec, DEK
 * unwrapper, and decryptor; surfaces as a deserialization failure and drives the source's fail-fast
 * behavior.
 *
 * <p>Extends {@link BadRecordException}: every condition that raises this exception (a malformed
 * envelope, an unsupported {@code alg}, an AEAD authentication-tag failure, ciphertext wrapped under
 * the wrong key) is attributable to the record's own bytes, not the environment.
 *
 * <p>Messages must never contain the plaintext DEK or decrypted payload.
 */
public class PayloadDecryptionException extends BadRecordException {

  public PayloadDecryptionException(String message) {
    super(message);
  }

  public PayloadDecryptionException(String message, Throwable cause) {
    super(message, cause);
  }
}
