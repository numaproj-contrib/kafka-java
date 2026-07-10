package io.numaproj.kafka.encryption;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.testing.FakeTicker;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DekCacheTest {

  private static final byte[] WRAPPED = {1, 2, 3, 4};
  private static final byte[] DEK = {9, 9, 9};

  @Test
  void loadsOnMissThenServesFromCache() {
    DekCache cache = new DekCache(1000, new FakeTicker());
    AtomicInteger loads = new AtomicInteger();

    byte[] first = cache.getOrLoad(WRAPPED, () -> countingLoad(loads, DEK));
    byte[] second = cache.getOrLoad(WRAPPED, () -> countingLoad(loads, DEK));

    assertArrayEquals(DEK, first);
    assertArrayEquals(DEK, second);
    assertEquals(1, loads.get()); // loader runs once; the second call is a cache hit
  }

  @Test
  void keysByContentNotArrayIdentity() {
    DekCache cache = new DekCache(1000, new FakeTicker());
    cache.getOrLoad(WRAPPED, () -> DEK);

    // A distinct array instance with identical content hits the same entry (loader must not run).
    byte[] hit = cache.getOrLoad(new byte[] {1, 2, 3, 4}, DekCacheTest::mustNotLoad);
    assertArrayEquals(DEK, hit);
  }

  @Test
  void distinctWrappedDeksAreIndependent() {
    DekCache cache = new DekCache(1000, new FakeTicker());
    byte[] other = {7, 7};
    cache.getOrLoad(WRAPPED, () -> DEK);

    assertArrayEquals(other, cache.getOrLoad(new byte[] {5, 6, 7, 8}, () -> other));
  }

  @Test
  void reloadsAfterTtlExpiry() {
    FakeTicker ticker = new FakeTicker();
    DekCache cache = new DekCache(1000, ticker);
    AtomicInteger loads = new AtomicInteger();

    cache.getOrLoad(WRAPPED, () -> countingLoad(loads, DEK));
    ticker.advance(999, TimeUnit.MILLISECONDS);
    cache.getOrLoad(WRAPPED, () -> countingLoad(loads, DEK)); // within TTL -> cache hit
    assertEquals(1, loads.get());

    ticker.advance(2, TimeUnit.MILLISECONDS); // past TTL
    cache.getOrLoad(WRAPPED, () -> countingLoad(loads, DEK)); // reloads
    assertEquals(2, loads.get());
  }

  @Test
  void propagatesLoaderFailure() {
    DekCache cache = new DekCache(1000, new FakeTicker());
    assertThrows(
        PayloadDecryptionException.class,
        () ->
            cache.getOrLoad(
                WRAPPED,
                () -> {
                  throw new PayloadDecryptionException("boom");
                }));
  }

  private static byte[] countingLoad(AtomicInteger counter, byte[] value) {
    counter.incrementAndGet();
    return value;
  }

  private static byte[] mustNotLoad() {
    throw new AssertionError("loader should not be called on a cache hit");
  }
}
