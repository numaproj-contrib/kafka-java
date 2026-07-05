package io.numaproj.kafka.crypto;

import java.time.Clock;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory TTL cache of plaintext DEKs, keyed by the wrapped-DEK bytes (not by topic): the wrapped
 * DEK changes on producer restart, and two producers may briefly use different DEKs on one topic
 * (spec §7.2 P2). A cache hit avoids a KMS call.
 *
 * <p>The {@link Clock} is injected so TTL expiry is testable. Plaintext DEKs held here must never be
 * logged (spec SR1).
 */
class DekCache {

  private final long ttlMillis;
  private final Clock clock;
  private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();

  private record Entry(byte[] dek, long expiresAtMillis) {}

  DekCache(long ttlMillis, Clock clock) {
    this.ttlMillis = ttlMillis;
    this.clock = clock;
  }

  /** Returns the cached plaintext DEK, or {@code null} if absent or expired. */
  byte[] get(byte[] wrappedDek) {
    String key = key(wrappedDek);
    Entry entry = entries.get(key);
    if (entry == null) {
      return null;
    }
    if (clock.millis() >= entry.expiresAtMillis()) {
      entries.remove(key, entry);
      return null;
    }
    return entry.dek();
  }

  void put(byte[] wrappedDek, byte[] dek) {
    entries.put(key(wrappedDek), new Entry(dek, clock.millis() + ttlMillis));
  }

  private static String key(byte[] wrappedDek) {
    return Base64.getEncoder().encodeToString(wrappedDek);
  }
}
