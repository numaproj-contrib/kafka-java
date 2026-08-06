package io.numaproj.kafka.encryption;

import com.google.common.base.Ticker;
import java.util.concurrent.TimeUnit;

/**
 * A {@link DekSource} that reuses one data encryption key for a bounded window, then generates a
 * fresh one. A decorator over any {@link DekGenerator}, so key reuse is backend-agnostic — the
 * mirror of the consumer-side DEK cache, which bounds how long a recovered DEK is held.
 *
 * <p>The consumer needs no coordination for this: every message carries its own wrapped DEK, so a
 * rotation is transparent. The reuse window is bounded both in time (the TTL) and in message count
 * ({@link #MAX_MESSAGES_PER_DEK}), because nonce uniqueness under a given key is what AES-GCM's
 * security rests on and random nonces bound collision probability per key only if the number of
 * encryptions per key is bounded.
 *
 * <p>Rotation is serialized so a burst of concurrent messages produces one key, not one per thread.
 * A rotated-out key's plaintext is erased from the heap as soon as its last {@link DekLease} is
 * released (see {@link LeasedDek}), and must never be logged.
 */
class RotatingDekGenerator implements DekSource {

  // NIST SP 800-38D: with random 96-bit nonces, keep encryptions per key well under 2^32 so the
  // nonce-collision probability stays negligible. This caps the count even when the TTL alone
  // would not (a very high-throughput topic within one window).
  static final long MAX_MESSAGES_PER_DEK = 1L << 22;

  private final DekGenerator delegate;
  private final long ttlNanos;
  private final Ticker ticker;

  private LeasedDek current;
  private long expiresAtNanos;
  private long messagesUnderCurrent;
  private boolean closed;

  RotatingDekGenerator(DekGenerator delegate, long ttlMillis, Ticker ticker) {
    this.delegate = delegate;
    this.ttlNanos = TimeUnit.MILLISECONDS.toNanos(ttlMillis);
    this.ticker = ticker;
  }

  @Override
  public synchronized DekLease acquire() {
    if (closed) {
      throw new IllegalStateException("the DEK source is closed");
    }
    long now = ticker.read();
    if (current == null
        || now - expiresAtNanos >= 0
        || messagesUnderCurrent >= MAX_MESSAGES_PER_DEK) {
      if (current != null) {
        current.retire();
      }
      current = new LeasedDek(delegate.generate());
      expiresAtNanos = now + ttlNanos;
      messagesUnderCurrent = 0;
    }
    messagesUnderCurrent++;
    return current.lease();
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    if (current != null) {
      current.retire();
      current = null;
    }
    delegate.close();
  }
}
