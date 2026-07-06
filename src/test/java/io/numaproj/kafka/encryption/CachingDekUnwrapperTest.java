package io.numaproj.kafka.encryption;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CachingDekUnwrapperTest {

  private static final byte[] WRAPPED = {1, 2, 3, 4};
  private static final byte[] DEK = new byte[32];

  @Mock private DekUnwrapper delegate;

  @Test
  void servesFromCacheWithinTtl() {
    when(delegate.unwrap(any())).thenReturn(DEK);
    AdjustableClock clock = new AdjustableClock(0);
    CachingDekUnwrapper unwrapper = new CachingDekUnwrapper(delegate, new DekCache(1000, clock));

    assertArrayEquals(DEK, unwrapper.unwrap(WRAPPED));
    assertArrayEquals(DEK, unwrapper.unwrap(WRAPPED));

    verify(delegate, times(1)).unwrap(any());
  }

  @Test
  void refetchesAfterTtlExpiry() {
    when(delegate.unwrap(any())).thenReturn(DEK);
    AdjustableClock clock = new AdjustableClock(0);
    CachingDekUnwrapper unwrapper = new CachingDekUnwrapper(delegate, new DekCache(1000, clock));

    unwrapper.unwrap(WRAPPED);
    clock.advance(1001); // past TTL
    unwrapper.unwrap(WRAPPED);

    verify(delegate, times(2)).unwrap(any());
  }

  @Test
  void closeClosesDelegate() {
    CachingDekUnwrapper unwrapper =
        new CachingDekUnwrapper(delegate, new DekCache(1000, new AdjustableClock(0)));
    unwrapper.close();
    verify(delegate).close();
  }
}
