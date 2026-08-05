package io.numaproj.kafka.encryption;

/**
 * Reads and writes the encryption envelope wire format. A codec owns the wire layout only (field
 * names/positions, encoding, its own version field); it performs no key-unwrap or key-generate calls,
 * and no encryption or decryption.
 */
public interface EnvelopeCodec {

  /**
   * Parse a Kafka message value into an {@link Envelope}.
   *
   * @throws PayloadDecryptionException if the value is not a well-formed envelope
   */
  Envelope parse(byte[] value);

  /**
   * Render an {@link Envelope} as the Kafka message value. The inverse of {@link #parse(byte[])}: what
   * this writes must be readable by a consumer using the same codec.
   */
  byte[] serialize(Envelope envelope);
}
