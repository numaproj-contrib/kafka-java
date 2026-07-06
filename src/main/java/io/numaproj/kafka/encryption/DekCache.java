package io.numaproj.kafka.encryption;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.time.Duration;
import java.util.Base64;

/**
 * In-memory TTL cache of plaintext DEKs, keyed by the wrapped-DEK bytes (not by topic): the wrapped
 * DEK changes on producer restart, and two producers may briefly use different DEKs on one topic. A
 * cache hit avoids an unwrap call to the key-management backend.
 *
 * <p>Backed by a Guava {@link Cache} for TTL expiry (including of entries that are never re-read)
 * and a bounded size. Plaintext DEKs held here must never be logged.
 */
class DekCache {

  /** Upper bound on distinct cached DEKs; caps memory as producers rotate keys over time. */
  private static final long MAX_ENTRIES = 1024;

  private final Cache<String, byte[]> cache;

  DekCache(long ttlMillis) {
    this(ttlMillis, Ticker.systemTicker());
  }

  /** Test seam: inject a {@link Ticker} to drive TTL expiry deterministically. */
  @VisibleForTesting
  DekCache(long ttlMillis, Ticker ticker) {
    this.cache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(ttlMillis))
            .maximumSize(MAX_ENTRIES)
            .ticker(ticker)
            .build();
  }

  /** Returns the cached plaintext DEK, or {@code null} if absent or expired. */
  byte[] get(byte[] wrappedDek) {
    return cache.getIfPresent(key(wrappedDek));
  }

  void put(byte[] wrappedDek, byte[] dek) {
    cache.put(key(wrappedDek), dek);
  }

  private static String key(byte[] wrappedDek) {
    return Base64.getEncoder().encodeToString(wrappedDek);
  }
}
