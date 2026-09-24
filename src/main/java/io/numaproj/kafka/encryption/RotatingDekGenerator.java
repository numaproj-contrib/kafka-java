package io.numaproj.kafka.encryption;

import java.util.Arrays;

/**
 * Reuses one data encryption key across many messages, rotating to a fresh one after a bounded number
 * of encryptions. A decorator over any {@link DekGenerator}, so reuse and rotation are
 * backend-agnostic. Consumers need no coordination, since every message carries its own wrapped DEK.
 *
 * <p><b>Why the bound matters.</b> {@link PayloadEncryptor} draws a random 96-bit nonce per message.
 * AES-GCM requires a unique nonce for every encryption under a given key; random 96-bit nonces begin
 * to collide (birthday bound) as the number of encryptions under one key approaches ~2^32 (NIST SP
 * 800-38D). A collision under the same key is catastrophic - it leaks the XOR of the affected
 * plaintexts and enables tag forgery. Rotating the DEK after {@code maxMessagesPerDek} encryptions
 * bounds the per-key encryption count, and therefore the collision probability, directly. The hazard
 * is a function of encryptions-per-key, not elapsed time, so rotation is driven by a message count.
 *
 * <p>Generation is serialized so a burst of concurrent first messages produces one key, not one per
 * thread. There is no retry here: a failed generation fails that message, which Numaflow redelivers.
 *
 * <p>The plaintext of a superseded DEK is erased (zero-filled) on rotation, and the current DEK is
 * erased on {@link #close()}, which runs only after the sinker has terminated - so no encryption can
 * still be using the key. It must never be logged.
 */
class RotatingDekGenerator implements DekGenerator {

  /**
   * Default rotation threshold: 2^24 (~16.7M) encryptions per DEK. A ~256x margin below the ~2^32
   * safe ceiling for random 96-bit nonces, so the birthday-collision probability stays negligible.
   * At one KMS GenerateDataKey per rotation, the backend cost is one call per ~16.7M messages.
   */
  static final long DEFAULT_MAX_MESSAGES_PER_DEK = 1L << 24;

  private final DekGenerator delegate;
  private final long maxMessagesPerDek;

  private Dek current;
  // Number of times the current DEK has been handed out (== encryptions it will be used for).
  private long currentUsageCount;
  private boolean closed;

  RotatingDekGenerator(DekGenerator delegate) {
    this(delegate, DEFAULT_MAX_MESSAGES_PER_DEK);
  }

  RotatingDekGenerator(DekGenerator delegate, long maxMessagesPerDek) {
    if (maxMessagesPerDek <= 0) {
      throw new IllegalArgumentException(
          "maxMessagesPerDek must be positive, got: " + maxMessagesPerDek);
    }
    this.delegate = delegate;
    this.maxMessagesPerDek = maxMessagesPerDek;
  }

  @Override
  public synchronized Dek generate() {
    if (closed) {
      throw new IllegalStateException("the DEK generator is closed");
    }
    if (current == null || currentUsageCount >= maxMessagesPerDek) {
      rotate();
    }
    currentUsageCount++;
    return current;
  }

  /** Erases the outgoing DEK's plaintext and generates a fresh one, resetting the usage count. */
  private void rotate() {
    eraseCurrent();
    current = delegate.generate();
    currentUsageCount = 0;
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    eraseCurrent();
    delegate.close();
  }

  /** Best-effort erasure of the held key material rather than leaving it for GC (heap dumps). */
  private void eraseCurrent() {
    if (current != null) {
      Arrays.fill(current.plaintext(), (byte) 0);
      current = null;
    }
  }
}
