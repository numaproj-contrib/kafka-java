package io.numaproj.kafka.encryption;

import java.util.Arrays;

/**
 * One DEK together with the number of live leases on it. The plaintext is erased (zero-filled) at
 * whichever comes last: retirement, or the release of the final lease. So a caller that acquired the
 * key just before it was rotated out still holds live key bytes until its lease closes, and the key
 * is scrubbed from the heap the moment nobody can be using it.
 *
 * <p>Erasure is best-effort by nature on a JVM (the GC may have copied the array, and JCE key specs
 * clone the bytes transiently); what this removes is long-lived plaintext keys accumulating in the
 * heap across rotations.
 */
final class LeasedDek {

  private final Dek dek;
  private int leases;
  private boolean retired;

  LeasedDek(Dek dek) {
    this.dek = dek;
  }

  synchronized DekLease lease() {
    leases++;
    return new Lease();
  }

  /** Marks the DEK as no longer current. Erases it now if no lease is outstanding. */
  synchronized void retire() {
    retired = true;
    eraseIfUnused();
  }

  private synchronized void release() {
    leases--;
    eraseIfUnused();
  }

  private void eraseIfUnused() {
    if (retired && leases == 0) {
      Arrays.fill(dek.plaintext(), (byte) 0);
    }
  }

  private final class Lease implements DekLease {

    private boolean closed;

    @Override
    public Dek dek() {
      return dek;
    }

    @Override
    public synchronized void close() {
      if (closed) {
        return;
      }
      closed = true;
      release();
    }
  }
}
