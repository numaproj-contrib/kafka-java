package io.numaproj.kafka.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.numaproj.kafka.config.UserConfig;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The worker is format agnostic, so byte[] values are used to exercise its behavior. */
class KafkaWorkerTest {

  private static final String TOPIC = "test-topic";

  @SuppressWarnings("unchecked")
  private final KafkaConsumer<String, byte[]> consumer = mock(KafkaConsumer.class);

  private KafkaWorker<byte[]> worker;
  private Thread thread;

  @BeforeEach
  void setUp() {
    UserConfig userConfig = mock(UserConfig.class);
    when(userConfig.getTopicName()).thenReturn(TOPIC);
    worker = new KafkaWorker<>(userConfig, consumer);
    thread = new Thread(worker);
  }

  @AfterEach
  void tearDown() {
    thread.interrupt();
  }

  @Test
  void poll_returnsRecordsAndSkipsNullValues() throws Exception {
    when(consumer.poll(any())).thenReturn(records("a", null, "b"));
    thread.start();

    List<ConsumerRecord<String, byte[]>> got = worker.poll(1000);

    assertEquals(2, got.size());
  }

  @Test
  void commit_delegatesToConsumer() throws Exception {
    thread.start();
    worker.commit();
    verify(consumer).commitAsync(any(OffsetCommitCallback.class));
  }

  @Test
  void getPartitions_returnsAssignedPartitionsForTopic() {
    when(consumer.assignment())
        .thenReturn(
            Set.of(new TopicPartition(TOPIC, 1), new TopicPartition(TOPIC, 3),
                new TopicPartition("other", 9)));

    assertEquals(Set.of(1, 3), new HashSet<>(worker.getPartitions()));
  }

  private static ConsumerRecords<String, byte[]> records(String... values) {
    List<ConsumerRecord<String, byte[]>> list = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      byte[] value = values[i] == null ? null : values[i].getBytes();
      list.add(
          new ConsumerRecord<>(
              TOPIC, 1, i, 0L, TimestampType.CREATE_TIME, 0, 0, "k" + i, value,
              new RecordHeaders(), Optional.empty()));
    }
    return new ConsumerRecords<>(Map.of(new TopicPartition(TOPIC, 1), list));
  }
}
