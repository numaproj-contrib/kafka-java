package io.numaproj.kafka.encryption;

/**
 * A {@link DekUnwrapper} decorator that caches recovered plaintext DEKs (keyed by the wrapped-DEK
 * bytes) in front of a delegate, so repeated messages under the same wrapped DEK avoid a backend
 * unwrap call.
 *
 * <p>Caching is backend-agnostic: any {@link DekUnwrapper} gets it without implementing caching
 * itself. Cached plaintext DEKs are never logged.
 */
public class CachingDekUnwrapper implements DekUnwrapper {

  private final DekUnwrapper delegate;
  private final DekCache cache;

  public CachingDekUnwrapper(DekUnwrapper delegate, DekCache cache) {
    this.delegate = delegate;
    this.cache = cache;
  }

  @Override
  public byte[] unwrap(byte[] wrappedDek) {
    byte[] cached = cache.get(wrappedDek);
    if (cached != null) {
      return cached;
    }
    byte[] dek = delegate.unwrap(wrappedDek);
    cache.put(wrappedDek, dek);
    return dek;
  }

  @Override
  public void close() {
    delegate.close();
  }
}
