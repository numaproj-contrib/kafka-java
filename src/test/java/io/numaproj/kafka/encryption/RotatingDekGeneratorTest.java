package io.numaproj.kafka.encryption;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.testing.FakeTicker;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kms.model.KmsException;

class RotatingDekGeneratorTest {

  private static final long TTL_MS = 1_000;

  private final DekGenerator delegate = mock(DekGenerator.class);
  private final FakeTicker ticker = new FakeTicker();

  private RotatingDekGenerator underTest;

  @BeforeEach
  void setUp() {
    underTest = new RotatingDekGenerator(delegate, TTL_MS, ticker);
  }

  private static Dek dek(byte marker) {
    return new Dek(new byte[] {marker}, new byte[] {marker});
  }

  @Test
  void reusesOneDekWithinTheTtl() {
    Dek first = dek((byte) 1);
    when(delegate.generate()).thenReturn(first);

    assertSame(first, underTest.generate());
    ticker.advance(TTL_MS - 1, TimeUnit.MILLISECONDS);
    assertSame(first, underTest.generate());

    verify(delegate, times(1)).generate();
  }

  @Test
  void generatesAgainOnceTheTtlElapses() {
    Dek first = dek((byte) 1);
    Dek second = dek((byte) 2);
    when(delegate.generate()).thenReturn(first, second);

    assertSame(first, underTest.generate());
    ticker.advance(TTL_MS + 1, TimeUnit.MILLISECONDS);
    assertSame(second, underTest.generate());

    verify(delegate, times(2)).generate();
  }

  @Test
  void propagatesBackendFailure() {
    when(delegate.generate()).thenThrow(KmsException.builder().message("access denied").build());

    // The caller fails the message rather than producing it unencrypted.
    assertThrows(KmsException.class, () -> underTest.generate());
  }

  @Test
  void retriesAfterAFailedGeneration() {
    Dek good = dek((byte) 9);
    when(delegate.generate())
        .thenThrow(KmsException.builder().message("throttled").build())
        .thenReturn(good);

    assertThrows(KmsException.class, () -> underTest.generate());
    // A failure must not be cached as the current DEK.
    assertSame(good, underTest.generate());
  }

  @Test
  void closeClosesTheDelegate() {
    underTest.close();
    verify(delegate).close();
  }
}
