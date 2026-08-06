package io.numaproj.kafka.encryption;

import com.google.common.base.Ticker;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Reuses one data encryption key for a bounded window, then generates a fresh one. A decorator over
 * any {@link DekGenerator}, so key reuse is backend-agnostic — the mirror of the consumer-side DEK
 * cache, which bounds how long a recovered DEK is held.
 *
 * <p>The consumer needs no coordination for this: every message carries its own wrapped DEK, so a
 * rotation is transparent. The reuse window is bounded both in time (the TTL) and in message count
 * ({@link #MAX_MESSAGES_PER_DEK}), because nonce uniqueness under a given key is what AES-GCM's
 * security rests on and random nonces bound collision probability per key only if the number of
 * encryptions per key is bounded.
 *
 * <p>Generation is serialized so a burst of concurrent messages produces one key, not one per thread.
 * The plaintext DEK held here must never be logged.
 */
class RotatingDekGenerator implements DekGenerator {

  // NIST SP 800-38D: with random 96-bit nonces, keep encryptions per key well under 2^32 so the
  // nonce-collision probability stays negligible. This caps the count even when the TTL alone
  // would not (a very high-throughput topic within one window).
  static final long MAX_MESSAGES_PER_DEK = 1L << 22;

  private final DekGenerator delegate;
  private final long ttlNanos;
  private final Ticker ticker;

  private Dek current;
  private long expiresAtNanos;
  private long messagesUnderCurrent;

  RotatingDekGenerator(DekGenerator delegate, long ttlMillis, Ticker ticker) {
    this.delegate = delegate;
    this.ttlNanos = TimeUnit.MILLISECONDS.toNanos(ttlMillis);
    this.ticker = ticker;
  }

  @Override
  public synchronized Dek generate() {
    long now = ticker.read();
    if (current == null
        || now - expiresAtNanos >= 0
        || messagesUnderCurrent >= MAX_MESSAGES_PER_DEK) {
      current = delegate.generate();
      expiresAtNanos = now + ttlNanos;
      messagesUnderCurrent = 0;
    }
    messagesUnderCurrent++;
    return current;
  }

  @Override
  public synchronized void close() {
    // Best-effort erasure of the key material rather than leaving it for GC (heap dumps). Only on
    // close: a rotated-out DEK is not zeroed, because a caller may still hold it mid-encrypt and
    // zeroing under it would silently encrypt with an all-zero key.
    if (current != null) {
      Arrays.fill(current.plaintext(), (byte) 0);
      current = null;
    }
    delegate.close();
  }
}
