package io.numaproj.kafka.encryption;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kms.model.KmsException;

class RotatingDekGeneratorTest {

  private final DekGenerator delegate = mock(DekGenerator.class);

  private RotatingDekGenerator underTest;

  @BeforeEach
  void setUp() {
    underTest = new RotatingDekGenerator(delegate);
  }

  @Test
  void generatesOnceAndReusesTheDekUntilTheThreshold() {
    Dek dek = new Dek(new byte[] {1}, new byte[] {1});
    when(delegate.generate()).thenReturn(dek);

    assertSame(dek, underTest.generate());
    assertSame(dek, underTest.generate());
    assertSame(dek, underTest.generate());

    verify(delegate, times(1)).generate();
  }

  @Test
  void rotatesAfterMaxMessagesPerDek() {
    Dek first = new Dek(new byte[] {1}, new byte[] {1});
    Dek second = new Dek(new byte[] {2}, new byte[] {2});
    when(delegate.generate()).thenReturn(first, second);

    RotatingDekGenerator rotating = new RotatingDekGenerator(delegate, 2);

    // The first DEK is used for exactly maxMessagesPerDek encryptions.
    assertSame(first, rotating.generate());
    assertSame(first, rotating.generate());
    // The next encryption crosses the threshold and rotates to a fresh DEK.
    assertSame(second, rotating.generate());
    assertSame(second, rotating.generate());

    verify(delegate, times(2)).generate();
  }

  @Test
  void doesNotEraseTheSupersededDekOnRotation() {
    Dek first = new Dek(new byte[] {1, 2, 3, 4}, new byte[] {9});
    Dek second = new Dek(new byte[] {5, 6, 7, 8}, new byte[] {9});
    when(delegate.generate()).thenReturn(first, second);

    RotatingDekGenerator rotating = new RotatingDekGenerator(delegate, 1);
    rotating.generate(); // hands out first
    rotating.generate(); // crosses threshold -> rotates to second

    // An encryption on another thread may still be using first, so its key must stay intact.
    assertArrayEquals(new byte[] {1, 2, 3, 4}, first.plaintext());
  }

  @Test
  void rejectsNonPositiveThreshold() {
    assertThrows(IllegalArgumentException.class, () -> new RotatingDekGenerator(delegate, 0));
    assertThrows(IllegalArgumentException.class, () -> new RotatingDekGenerator(delegate, -1));
  }

  @Test
  void propagatesBackendFailure() {
    when(delegate.generate()).thenThrow(KmsException.builder().message("access denied").build());

    // The caller fails the message rather than producing it unencrypted.
    assertThrows(KmsException.class, () -> underTest.generate());
  }

  @Test
  void closeErasesTheHeldPlaintextDekAndClosesTheDelegate() {
    Dek dek = new Dek(new byte[] {1, 2, 3, 4}, new byte[] {9});
    when(delegate.generate()).thenReturn(dek);
    underTest.generate();

    underTest.close();

    assertArrayEquals(new byte[4], dek.plaintext());
    verify(delegate).close();
  }

  @Test
  void generateAfterCloseThrows() {
    underTest.close();
    assertThrows(IllegalStateException.class, () -> underTest.generate());
  }
}
