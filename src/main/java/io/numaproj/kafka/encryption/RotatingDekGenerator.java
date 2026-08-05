package io.numaproj.kafka.encryption;

import com.google.common.base.Ticker;
import java.util.concurrent.TimeUnit;

/**
 * Reuses one data encryption key for a bounded window, then generates a fresh one. A decorator over
 * any {@link DekGenerator}, so key reuse is backend-agnostic — the mirror of the consumer-side DEK
 * cache, which bounds how long a recovered DEK is held.
 *
 * <p>The consumer needs no coordination for this: every message carries its own wrapped DEK, so a
 * rotation is transparent. Bounding the window also bounds how many messages share a key, which
 * matters because nonce uniqueness under a given key is what AES-GCM's security rests on.
 *
 * <p>Generation is serialized so a burst of concurrent messages produces one key, not one per thread.
 * The plaintext DEK held here must never be logged.
 */
class RotatingDekGenerator implements DekGenerator {

  private final DekGenerator delegate;
  private final long ttlNanos;
  private final Ticker ticker;

  private Dek current;
  private long expiresAtNanos;

  RotatingDekGenerator(DekGenerator delegate, long ttlMillis, Ticker ticker) {
    this.delegate = delegate;
    this.ttlNanos = TimeUnit.MILLISECONDS.toNanos(ttlMillis);
    this.ticker = ticker;
  }

  @Override
  public synchronized Dek generate() {
    long now = ticker.read();
    if (current == null || now - expiresAtNanos >= 0) {
      current = delegate.generate();
      expiresAtNanos = now + ttlNanos;
    }
    return current;
  }

  @Override
  public void close() {
    delegate.close();
  }
}
