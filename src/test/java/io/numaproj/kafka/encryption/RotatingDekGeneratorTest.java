package io.numaproj.kafka.encryption;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

  private static boolean isErased(Dek dek) {
    for (byte b : dek.plaintext()) {
      if (b != 0) {
        return false;
      }
    }
    return true;
  }

  /** Acquires the current DEK and immediately releases the lease. */
  private Dek acquireAndRelease() {
    try (DekLease lease = underTest.acquire()) {
      return lease.dek();
    }
  }

  @Test
  void reusesOneDekWithinTheTtl() {
    Dek first = dek((byte) 1);
    when(delegate.generate()).thenReturn(first);

    assertSame(first, acquireAndRelease());
    ticker.advance(TTL_MS - 1, TimeUnit.MILLISECONDS);
    assertSame(first, acquireAndRelease());

    verify(delegate, times(1)).generate();
  }

  @Test
  void generatesAgainOnceTheTtlElapses() {
    Dek first = dek((byte) 1);
    Dek second = dek((byte) 2);
    when(delegate.generate()).thenReturn(first, second);

    assertSame(first, acquireAndRelease());
    ticker.advance(TTL_MS + 1, TimeUnit.MILLISECONDS);
    assertSame(second, acquireAndRelease());

    verify(delegate, times(2)).generate();
  }

  @Test
  void generatesAgainOnceTheMessageCapIsReached() {
    // Even within the TTL, a DEK must not be used for more than MAX_MESSAGES_PER_DEK encryptions
    // (random-nonce collision bound).
    Dek first = dek((byte) 1);
    Dek second = dek((byte) 2);
    when(delegate.generate()).thenReturn(first, second);

    for (long i = 0; i < RotatingDekGenerator.MAX_MESSAGES_PER_DEK; i++) {
      assertSame(first, acquireAndRelease());
    }
    assertSame(second, acquireAndRelease());

    verify(delegate, times(2)).generate();
  }

  @Test
  void propagatesBackendFailure() {
    when(delegate.generate()).thenThrow(KmsException.builder().message("access denied").build());

    // The caller fails the message rather than producing it unencrypted.
    assertThrows(KmsException.class, () -> underTest.acquire());
  }

  @Test
  void retriesAfterAFailedGeneration() {
    Dek good = dek((byte) 9);
    when(delegate.generate())
        .thenThrow(KmsException.builder().message("throttled").build())
        .thenReturn(good);

    assertThrows(KmsException.class, () -> underTest.acquire());
    // A failure must not be cached as the current DEK.
    assertSame(good, acquireAndRelease());
  }

  @Test
  void closeClosesTheDelegate() {
    underTest.close();
    verify(delegate).close();
  }

  @Test
  void closeErasesTheHeldPlaintextDek() {
    Dek current = new Dek(new byte[] {1, 2, 3, 4}, new byte[] {9});
    when(delegate.generate()).thenReturn(current);
    acquireAndRelease();

    underTest.close();

    assertArrayEquals(new byte[4], current.plaintext());
  }

  @Test
  void erasesARotatedOutDekOnceItsLastLeaseIsReleased() {
    Dek first = new Dek(new byte[] {1, 2, 3, 4}, new byte[] {1});
    Dek second = dek((byte) 2);
    when(delegate.generate()).thenReturn(first, second);

    DekLease outstanding = underTest.acquire();
    ticker.advance(TTL_MS + 1, TimeUnit.MILLISECONDS);
    assertSame(second, acquireAndRelease()); // rotates first out

    // The old key must stay live while a lease on it is open: the holder may be mid-encrypt.
    assertFalse(isErased(first), "must not erase under an open lease");

    outstanding.close();
    assertTrue(isErased(first), "erased as soon as the last lease is released");
    assertFalse(isErased(second), "the current DEK stays live");
  }

  @Test
  void closeDefersErasureToTheOutstandingLease() {
    Dek current = new Dek(new byte[] {1, 2, 3, 4}, new byte[] {9});
    when(delegate.generate()).thenReturn(current);

    DekLease outstanding = underTest.acquire();
    underTest.close();
    assertFalse(isErased(current), "must not erase under an open lease");

    outstanding.close();
    assertTrue(isErased(current));
  }

  @Test
  void leaseCloseIsIdempotent() {
    Dek first = new Dek(new byte[] {1, 2, 3, 4}, new byte[] {1});
    when(delegate.generate()).thenReturn(first);

    DekLease a = underTest.acquire();
    DekLease b = underTest.acquire();
    a.close();
    a.close(); // double-close must not release b's reference

    underTest.close();
    assertFalse(isErased(first), "b still holds the key");

    b.close();
    assertTrue(isErased(first));
  }

  @Test
  void acquireAfterCloseThrows() {
    underTest.close();
    assertThrows(IllegalStateException.class, () -> underTest.acquire());
  }
}
