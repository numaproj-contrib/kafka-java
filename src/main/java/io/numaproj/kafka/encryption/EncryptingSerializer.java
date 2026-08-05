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
 * {@link DecryptingDeserializer}, which decrypts before delegating.
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
    return encrypt(delegate.serialize(topic, data));
  }

  @Override
  public byte[] serialize(String topic, Headers headers, T data) {
    return encrypt(delegate.serialize(topic, headers, data));
  }

  /** A null or empty serialization (e.g. a tombstone) is passed through, not encrypted. */
  private byte[] encrypt(byte[] serialized) {
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
