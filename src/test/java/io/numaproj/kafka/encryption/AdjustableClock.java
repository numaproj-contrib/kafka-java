package io.numaproj.kafka.encryption;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/** A test {@link Clock} whose time advances only when told to, for deterministic TTL tests. */
class AdjustableClock extends Clock {

  private long millis;

  AdjustableClock(long startMillis) {
    this.millis = startMillis;
  }

  void advance(long deltaMillis) {
    this.millis += deltaMillis;
  }

  @Override
  public long millis() {
    return millis;
  }

  @Override
  public Instant instant() {
    return Instant.ofEpochMilli(millis);
  }

  @Override
  public ZoneId getZone() {
    return ZoneOffset.UTC;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return this;
  }
}
