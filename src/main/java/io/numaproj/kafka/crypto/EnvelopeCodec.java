package io.numaproj.kafka.crypto;

/**
 * Parses the encryption envelope wire format into structured fields. A codec owns the wire layout
 * only (field names/positions, encoding, its own version field); it performs no key-unwrap calls and
 * no decryption (spec EC1).
 *
 * <p>Internal seam, not a public extension point (spec RD1).
 */
@FunctionalInterface
public interface EnvelopeCodec {

  /**
   * Parse a Kafka message value into an {@link Envelope}.
   *
   * @throws PayloadDecryptionException if the value is not a well-formed envelope
   */
  Envelope parse(String topic, byte[] value);
}
