package io.numaproj.kafka.encryption;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.testing.FakeTicker;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class DekCacheTest {

  private static final byte[] WRAPPED = {1, 2, 3, 4};
  private static final byte[] DEK = {9, 9, 9};

  @Test
  void returnsNullWhenAbsent() {
    DekCache cache = new DekCache(1000, new FakeTicker());
    assertNull(cache.get(WRAPPED));
  }

  @Test
  void returnsCachedDekWithinTtl() {
    DekCache cache = new DekCache(1000, new FakeTicker());
    cache.put(WRAPPED, DEK);
    assertArrayEquals(DEK, cache.get(WRAPPED));
  }

  @Test
  void keysByContentNotArrayIdentity() {
    DekCache cache = new DekCache(1000, new FakeTicker());
    cache.put(WRAPPED, DEK);
    // A distinct array instance with identical content must hit the same entry.
    assertArrayEquals(DEK, cache.get(new byte[] {1, 2, 3, 4}));
  }

  @Test
  void distinctWrappedDeksAreIndependent() {
    DekCache cache = new DekCache(1000, new FakeTicker());
    cache.put(WRAPPED, DEK);
    assertNull(cache.get(new byte[] {5, 6, 7, 8}));
  }

  @Test
  void expiresAfterTtl() {
    FakeTicker ticker = new FakeTicker();
    DekCache cache = new DekCache(1000, ticker);
    cache.put(WRAPPED, DEK);

    ticker.advance(999, TimeUnit.MILLISECONDS);
    assertArrayEquals(DEK, cache.get(WRAPPED)); // still within TTL

    ticker.advance(2, TimeUnit.MILLISECONDS); // now past TTL
    assertNull(cache.get(WRAPPED));
  }
}
