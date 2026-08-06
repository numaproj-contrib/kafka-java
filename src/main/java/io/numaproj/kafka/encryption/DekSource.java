package io.numaproj.kafka.encryption;

/**
 * Hands out leases on a current data encryption key. This is the interface the encrypt path
 * consumes: the lease protocol lets an implementation erase retired key material as soon as it is
 * provably unused, without callers coordinating beyond try-with-resources.
 *
 * <p>How a "current" key comes to be — which backend generates it, and when it rotates — is the
 * implementation's concern; see {@link RotatingDekGenerator}.
 */
public interface DekSource {

  /** Borrow the current DEK. The caller must close the lease once done with the key bytes. */
  DekLease acquire();

  /** Release resources. The current DEK is erased once its outstanding leases are released. */
  void close();
}
