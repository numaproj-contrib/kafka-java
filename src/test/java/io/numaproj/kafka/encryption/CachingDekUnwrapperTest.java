package io.numaproj.kafka.encryption;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.testing.FakeTicker;
import java.util.concurrent.TimeUnit;
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
    FakeTicker ticker = new FakeTicker();
    CachingDekUnwrapper unwrapper = new CachingDekUnwrapper(delegate, new DekCache(1000, ticker));

    assertArrayEquals(DEK, unwrapper.unwrap(WRAPPED));
    assertArrayEquals(DEK, unwrapper.unwrap(WRAPPED));

    verify(delegate, times(1)).unwrap(any());
  }

  @Test
  void refetchesAfterTtlExpiry() {
    when(delegate.unwrap(any())).thenReturn(DEK);
    FakeTicker ticker = new FakeTicker();
    CachingDekUnwrapper unwrapper = new CachingDekUnwrapper(delegate, new DekCache(1000, ticker));

    unwrapper.unwrap(WRAPPED);
    ticker.advance(1001, TimeUnit.MILLISECONDS); // past TTL
    unwrapper.unwrap(WRAPPED);

    verify(delegate, times(2)).unwrap(any());
  }

  @Test
  void closeClosesDelegate() {
    CachingDekUnwrapper unwrapper =
        new CachingDekUnwrapper(delegate, new DekCache(1000, new FakeTicker()));
    unwrapper.close();
    verify(delegate).close();
  }
}
