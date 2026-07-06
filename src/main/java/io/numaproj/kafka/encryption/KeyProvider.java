package io.numaproj.kafka.encryption;

/**
 * Unwraps a wrapped data encryption key (DEK) into its plaintext form via a key-management backend.
 * A provider owns key unwrapping and DEK caching only; it is independent of the wire format (spec
 * KP1).
 *
 * <p>Internal seam, not a public extension point (spec RD1).
 */
public interface KeyProvider {

  /**
   * Recover the plaintext DEK from its wrapped form.
   *
   * @throws PayloadDecryptionException or a backend exception if unwrapping fails
   */
  byte[] unwrap(byte[] wrappedDek);

  /** Release any underlying clients/resources. */
  void close();
}
