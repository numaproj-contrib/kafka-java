package io.numaproj.kafka.encryption;

/**
 * Produces data encryption keys from a key-management backend. The mirror of {@link DekUnwrapper} on
 * the produce side: the backend generates a DEK and returns it both in plaintext and wrapped under a
 * key the backend holds.
 *
 * <p>An implementation owns key generation only. DEK reuse is a backend-agnostic concern, applied
 * by {@link ProcessLifetimeDekGenerator} in front of any generator.
 */
public interface DekGenerator extends AutoCloseable {

  /**
   * Generate a new data encryption key.
   *
   * @throws RuntimeException if the backend call fails (the caller fails the message rather than
   *     producing it unencrypted)
   */
  Dek generate();

  /** Release resources (clients/credentials). */
  void close();
}
