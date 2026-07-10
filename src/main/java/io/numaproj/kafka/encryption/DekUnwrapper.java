package io.numaproj.kafka.encryption;

/**
 * Unwraps a wrapped data encryption key (DEK) into its plaintext form via a key-management backend.
 */
public interface DekUnwrapper {

  /**
   * Recover the plaintext DEK from its wrapped form.
   *
   * @throws RuntimeException if unwrapping fails
   */
  byte[] unwrap(byte[] wrappedDek);

  /** Release any underlying clients/resources. */
  void close();
}
