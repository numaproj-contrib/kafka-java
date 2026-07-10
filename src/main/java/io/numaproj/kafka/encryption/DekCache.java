package io.numaproj.kafka.encryption;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

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

  DekCache(long ttlMillis, Ticker ticker) {
    this.cache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(ttlMillis))
            .maximumSize(MAX_ENTRIES)
            .ticker(ticker)
            .build();
  }

  /**
   * Returns the cached plaintext DEK for {@code wrappedDek}, or atomically computes it via {@code
   * loader} and caches it. The loader runs at most once per key, even under concurrent access, so
   * repeated messages under the same wrapped DEK make a single backend unwrap call (no stampede).
   */
  byte[] getOrLoad(byte[] wrappedDek, Callable<byte[]> loader) {
    try {
      return cache.get(key(wrappedDek), loader);
    } catch (ExecutionException | UncheckedExecutionException e) {
      // Preserve fail-fast: surface the loader's original exception (e.g. KmsException,
      // PayloadDecryptionException) rather than Guava's wrapper.
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      throw new PayloadDecryptionException("DEK unwrap failed", cause);
    }
  }

  private static String key(byte[] wrappedDek) {
    return Base64.getEncoder().encodeToString(wrappedDek);
  }
}
