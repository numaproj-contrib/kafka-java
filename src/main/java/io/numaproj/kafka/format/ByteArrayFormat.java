package io.numaproj.kafka.format;

/**
 * Raw byte-array format: payloads pass through unchanged in both directions. Used when the topic has
 * no schema associated with it.
 */
public class ByteArrayFormat implements KafkaFormat<byte[]> {

  @Override
  public byte[] toPayload(byte[] value) {
    return value;
  }

  @Override
  public byte[] toRecord(byte[] payload) {
    return payload;
  }
}
