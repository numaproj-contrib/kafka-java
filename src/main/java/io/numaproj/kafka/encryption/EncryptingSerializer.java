package io.numaproj.kafka.encryption;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A Kafka {@link Serializer} that delegates to the serializer the sink would otherwise use (Glue Avro,
 * Confluent Avro, or ByteArray), then encrypts the bytes it produced. Because it delegates, it works
 * for every serialization path.
 *
 * <p>Sitting on the outside of the delegate is what makes encryption the <em>final</em> step: the
 * value on the wire is {@code encrypt(serialize(record))}. The mirror of
 * {@link DecryptingDeserializer}, which decrypts before delegating, down to how a null or empty
 * serialized value is handled — both are written through unencrypted, for two different reasons.
 *
 * <p>A null value is a tombstone, and it only marks a key for deletion while it stays null on the
 * wire; there is no envelope that could carry it. An empty value <em>could</em> be encrypted — the
 * cipher accepts an empty plaintext and returns the authentication tag — but is not, so that the
 * raw sink's round trip stays symmetric with the source, which passes an empty value to its
 * delegate undecrypted. Neither case loses protection: an empty plaintext carries no information,
 * and the envelope would advertise its length regardless.
 *
 * <p>The delegate is configured by the caller ({@code ProducerConfig}) before being wrapped, so this
 * wrapper does not override {@link #configure}; Kafka's inherited no-op is sufficient.
 */
public class EncryptingSerializer<T> implements Serializer<T> {

  private final Serializer<T> delegate;
  private final PayloadEncryptor encryptor;

  public EncryptingSerializer(Serializer<T> delegate, PayloadEncryptor encryptor) {
    this.delegate = delegate;
    this.encryptor = encryptor;
  }

  @Override
  public byte[] serialize(String topic, T data) {
    return encryptIfPresent(delegate.serialize(topic, data));
  }

  @Override
  public byte[] serialize(String topic, Headers headers, T data) {
    return encryptIfPresent(delegate.serialize(topic, headers, data));
  }

  private byte[] encryptIfPresent(byte[] serialized) {
    // Null is guarded because it has to be: the cipher rejects a null input buffer with an
    // IllegalArgumentException, which PayloadEncryptor does not catch, so a tombstone would fail the
    // message with an opaque error after spending a DEK generation to get there. Empty is guarded by
    // choice: it encrypts fine, but leaving it alone keeps the sink symmetric with
    // DecryptingDeserializer, which hands an empty value to its delegate without decrypting.
    if (serialized == null || serialized.length == 0) {
      return serialized;
    }
    return encryptor.encrypt(serialized);
  }

  @Override
  public void close() {
    try {
      encryptor.close();
    } finally {
      delegate.close();
    }
  }
}
