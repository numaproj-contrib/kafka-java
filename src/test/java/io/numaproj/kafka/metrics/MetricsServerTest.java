package io.numaproj.kafka.metrics;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class MetricsServerTest {

  @Test
  void resolvePort_unset_returnsDefault() {
    assertEquals(MetricsServer.DEFAULT_PORT, MetricsServer.resolvePort(null));
  }

  @Test
  void resolvePort_blank_returnsDefault() {
    assertEquals(MetricsServer.DEFAULT_PORT, MetricsServer.resolvePort("  "));
  }

  @Test
  void resolvePort_validPort_returnsIt() {
    assertEquals(8080, MetricsServer.resolvePort("8080"));
  }

  @Test
  void resolvePort_trimsWhitespace() {
    assertEquals(8080, MetricsServer.resolvePort(" 8080 "));
  }

  @Test
  void resolvePort_zero_meansDisabled() {
    assertEquals(0, MetricsServer.resolvePort("0"));
  }

  @Test
  void resolvePort_nonInteger_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> MetricsServer.resolvePort("http"));
    assertTrue(e.getMessage().contains("http"));
  }

  @Test
  void resolvePort_outOfRange_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> MetricsServer.resolvePort("-1"));
    assertThrows(IllegalArgumentException.class, () -> MetricsServer.resolvePort("65536"));
  }
}
