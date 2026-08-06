package io.numaproj.kafka.encryption;

/** Test double: a {@link DekSource} that always leases the same fixed DEK and erases nothing. */
public final class FixedDekSource implements DekSource {

  private final Dek dek;

  public FixedDekSource(byte[] plaintext, byte[] wrapped) {
    this.dek = new Dek(plaintext, wrapped);
  }

  @Override
  public DekLease acquire() {
    return new DekLease() {
      @Override
      public Dek dek() {
        return dek;
      }

      @Override
      public void close() {}
    };
  }

  @Override
  public void close() {}
}
