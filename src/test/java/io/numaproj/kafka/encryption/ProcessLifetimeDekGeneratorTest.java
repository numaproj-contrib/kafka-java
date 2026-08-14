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

class ProcessLifetimeDekGeneratorTest {

  private final DekGenerator delegate = mock(DekGenerator.class);

  private ProcessLifetimeDekGenerator underTest;

  @BeforeEach
  void setUp() {
    underTest = new ProcessLifetimeDekGenerator(delegate);
  }

  @Test
  void generatesOnceAndReusesTheDekForTheProcessLifetime() {
    Dek dek = new Dek(new byte[] {1}, new byte[] {1});
    when(delegate.generate()).thenReturn(dek);

    assertSame(dek, underTest.generate());
    assertSame(dek, underTest.generate());
    assertSame(dek, underTest.generate());

    verify(delegate, times(1)).generate();
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
