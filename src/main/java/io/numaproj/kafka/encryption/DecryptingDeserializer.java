package io.numaproj.kafka.encryption;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * A Kafka {@link Deserializer} that decrypts the value, then delegates to the deserializer the source
 * would otherwise use (Glue Avro, Confluent Avro, or ByteArray). Because it delegates, it works for
 * every serialization path.
 *
 * <p>The delegate is configured by the caller ({@code ConsumerConfig}) before being wrapped, so this
 * wrapper does not override {@link #configure}; Kafka's inherited no-op is sufficient.
 */
public class DecryptingDeserializer<T> implements Deserializer<T> {

  private final Deserializer<T> delegate;
  private final PayloadDecryptor decryptor;

  public DecryptingDeserializer(Deserializer<T> delegate, PayloadDecryptor decryptor) {
    this.delegate = delegate;
    this.decryptor = decryptor;
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    if (data == null || data.length == 0) {
      return delegate.deserialize(topic, data);
    }
    return delegate.deserialize(topic, decryptor.decrypt(topic, data));
  }

  @Override
  public T deserialize(String topic, Headers headers, byte[] data) {
    if (data == null || data.length == 0) {
      return delegate.deserialize(topic, headers, data);
    }
    return delegate.deserialize(topic, headers, decryptor.decrypt(topic, data));
  }

  @Override
  public void close() {
    try {
      decryptor.close();
    } finally {
      delegate.close();
    }
  }
}
