package io.numaproj.kafka.encryption;

import java.util.Arrays;

/**
 * Reuses one data encryption key for the lifetime of the process: the DEK is generated on first use
 * and rotation happens by process restart, so a redeploy rotates it. Consumers need no coordination,
 * since every message carries its own wrapped DEK.
 *
 * <p>A decorator over any {@link DekGenerator}, so key reuse is backend-agnostic. Generation is
 * serialized so a burst of concurrent first messages produces one key, not one per thread. A failed
 * generation is not cached; the next message retries.
 *
 * <p>The plaintext is erased (zero-filled) on {@link #close()}, which runs only after the sinker has
 * terminated — so no encryption can still be using the key. It must never be logged.
 */
class ProcessLifetimeDekGenerator implements DekGenerator {

  private final DekGenerator delegate;

  private Dek current;
  private boolean closed;

  ProcessLifetimeDekGenerator(DekGenerator delegate) {
    this.delegate = delegate;
  }

  @Override
  public synchronized Dek generate() {
    if (closed) {
      throw new IllegalStateException("the DEK generator is closed");
    }
    if (current == null) {
      current = delegate.generate();
    }
    return current;
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    if (current != null) {
      // Best-effort erasure of the key material rather than leaving it for GC (heap dumps).
      Arrays.fill(current.plaintext(), (byte) 0);
      current = null;
    }
    delegate.close();
  }
}
