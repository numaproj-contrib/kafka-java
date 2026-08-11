package io.numaproj.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.RecordDeserializationException;

/**
 * Identifies a record by coordinates only, so no {@code ConsumerRecord} - and no decrypted value -
 * is ever rendered into a log.
 */
record RecordLocation(String topic, int partition, long offset) {

  static RecordLocation of(ConsumerRecord<?, ?> record) {
    return new RecordLocation(record.topic(), record.partition(), record.offset());
  }

  static RecordLocation of(RecordDeserializationException e) {
    return new RecordLocation(e.topicPartition().topic(), e.topicPartition().partition(), e.offset());
  }

  @Override
  public String toString() {
    return "topic:" + topic + " partition:" + partition + " offset:" + offset;
  }
}
