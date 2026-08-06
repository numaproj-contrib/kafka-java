package io.numaproj.kafka.encryption;

/**
 * A borrowed reference to a data encryption key. The plaintext is guaranteed live (not erased) from
 * acquisition until {@link #close()}, even if the key is rotated out in the meantime — which is what
 * makes prompt erasure of retired keys safe under concurrency. Always use in try-with-resources.
 */
public interface DekLease extends AutoCloseable {

  Dek dek();

  /** Releases the lease. Idempotent; no checked exception. */
  @Override
  void close();
}
