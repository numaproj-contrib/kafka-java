package io.numaproj.kafka.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.numaproj.kafka.config.UserConfig;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
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

  /** Builds the exception with the origin and buffers, as the Kafka consumer itself does. */
  private static RecordDeserializationException deserializationException(
      long offset, Throwable cause) {
    return new RecordDeserializationException(
        RecordDeserializationException.DeserializationExceptionOrigin.VALUE,
        new TopicPartition(TOPIC, 1),
        offset,
        0L,
        TimestampType.CREATE_TIME,
        ByteBuffer.allocate(0),
        ByteBuffer.allocate(0),
        new RecordHeaders(),
        "boom",
        cause);
  }

  @AfterEach
  void tearDown() {
    thread.interrupt();
  }

  @Test
  void poll_returnsEveryRecordPolled_includingTombstones() throws Exception {
    // Deciding what to do with a tombstone belongs to the sourcer, so the worker forwards it on.
    when(consumer.poll(any())).thenReturn(records("a", null, "b"));
    thread.start();

    List<ConsumerRecord<String, byte[]>> got = worker.poll(1000);

    assertEquals(3, got.size());
  }

  @Test
  void poll_whenConsumerThrows_thenPropagatesOriginalCauseAndLeavesInterruptFlagClear()
      throws Exception {
    RuntimeException boom = new RuntimeException("boom");
    when(consumer.poll(any())).thenThrow(boom);
    thread.start();

    RuntimeException thrown = assertThrows(RuntimeException.class, () -> worker.poll(1000));

    assertSame(boom, thrown);
    assertFalse(Thread.currentThread().isInterrupted());
  }

  @Test
  void poll_whenRecordCannotBeDeserialized_thenReportsItsLocationAndNeverSeeks() throws Exception {
    RuntimeException cause = new RuntimeException("bad avro");
    when(consumer.poll(any())).thenThrow(deserializationException(5L, cause));
    thread.start();

    PoisonRecordException thrown =
        assertThrows(PoisonRecordException.class, () -> worker.poll(1000));

    assertEquals(new RecordLocation(TOPIC, 1, 5L), thrown.location());
    assertSame(cause, thrown.getCause());
    // Recovering from it is the caller's decision, taken from onError.
    verify(consumer, never()).seek(any(), anyLong());
  }

  @Test
  void poll_whenTheDeserializationFailureHasNoCause_thenReportsTheKafkaWrapper() throws Exception {
    RecordDeserializationException noCause = deserializationException(5L, null);
    when(consumer.poll(any())).thenThrow(noCause);
    thread.start();

    PoisonRecordException thrown =
        assertThrows(PoisonRecordException.class, () -> worker.poll(1000));

    assertSame(noCause, thrown.getCause());
  }

  @Test
  void seekPast_movesTheConsumerOnePastTheRecord() throws Exception {
    thread.start();

    worker.seekPast(new RecordLocation(TOPIC, 1, 5L));

    verify(consumer).seek(new TopicPartition(TOPIC, 1), 6L);
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
