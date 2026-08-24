package io.numaproj.kafka.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.numaproj.kafka.config.OnError;
import io.numaproj.kafka.config.UserConfig;
import io.numaproj.kafka.metrics.SourceMetrics;
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
import org.mockito.ArgumentCaptor;

/** The worker is format agnostic, so byte[] values are used to exercise its behavior. */
class KafkaWorkerTest {

  private static final String TOPIC = "test-topic";

  @SuppressWarnings("unchecked")
  private final KafkaConsumer<String, byte[]> consumer = mock(KafkaConsumer.class);

  private final SourceMetrics metrics = mock(SourceMetrics.class);

  private KafkaWorker<byte[]> worker;
  private Thread thread;

  @BeforeEach
  void setUp() {
    worker = worker(OnError.FAIL);
    thread = new Thread(worker);
  }

  private KafkaWorker<byte[]> worker(OnError onError) {
    return worker(onError, new SkippedRecordHandler(metrics));
  }

  private KafkaWorker<byte[]> worker(OnError onError, SkippedRecordHandler handler) {
    UserConfig userConfig = mock(UserConfig.class);
    when(userConfig.getTopicName()).thenReturn(TOPIC);
    when(userConfig.getOnError()).thenReturn(onError);
    return new KafkaWorker<>(userConfig, consumer, handler);
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
  void poll_whenRecordCannotBeDeserializedAndOnErrorFail_thenThrowsAndNeverSeeks() throws Exception {
    when(consumer.poll(any())).thenThrow(deserializationException(5L, new RuntimeException("bad")));
    thread.start();

    assertThrows(RecordDeserializationException.class, () -> worker.poll(1000));

    verify(consumer, never()).seek(any(), anyLong());
    verify(metrics, never()).recordSkipped();
  }

  @Test
  void poll_whenRecordCannotBeDeserializedAndOnErrorSkip_thenSeeksCountsAndContinues()
      throws Exception {
    RuntimeException cause = new RuntimeException("bad avro");
    when(consumer.poll(any()))
        .thenThrow(deserializationException(5L, cause))
        .thenReturn(records("good"));
    KafkaWorker<byte[]> skipWorker = worker(OnError.SKIP);
    Thread skipThread = new Thread(skipWorker);
    skipThread.start();

    List<ConsumerRecord<String, byte[]>> got = skipWorker.poll(1000);

    verify(consumer).seek(new TopicPartition(TOPIC, 1), 6L);
    verify(metrics).recordSkipped();
    assertEquals(1, got.size());
    skipThread.interrupt();
  }

  @Test
  void poll_whenConsecutiveBadRecordsAndOnErrorSkip_thenSeeksPastEach() throws Exception {
    when(consumer.poll(any()))
        .thenThrow(deserializationException(5L, new RuntimeException("bad")))
        .thenThrow(deserializationException(6L, new RuntimeException("bad")))
        .thenReturn(records("good"));
    KafkaWorker<byte[]> skipWorker = worker(OnError.SKIP);
    Thread skipThread = new Thread(skipWorker);
    skipThread.start();

    List<ConsumerRecord<String, byte[]>> got = skipWorker.poll(1000);

    verify(consumer).seek(new TopicPartition(TOPIC, 1), 6L);
    verify(consumer).seek(new TopicPartition(TOPIC, 1), 7L);
    verify(metrics, times(2)).recordSkipped();
    assertEquals(1, got.size());
    skipThread.interrupt();
  }

  @Test
  void poll_whenRecordSkipped_thenTheFailureHandedOverCarriesNoFieldValues() throws Exception {
    // The drop is logged with this failure attached, and a deserializer's message embeds the
    // offending - possibly decrypted - field values.
    SkippedRecordHandler handler = mock(SkippedRecordHandler.class);
    RuntimeException cause = new RuntimeException("expected int for ssn, got 123-45-6789");
    when(consumer.poll(any()))
        .thenThrow(deserializationException(5L, cause))
        .thenReturn(records("good"));
    KafkaWorker<byte[]> skipWorker = worker(OnError.SKIP, handler);
    Thread skipThread = new Thread(skipWorker);
    skipThread.start();

    skipWorker.poll(1000);

    ArgumentCaptor<Throwable> failure = ArgumentCaptor.forClass(Throwable.class);
    verify(handler).handleSkipped(eq(TOPIC), eq(1), eq(5L), failure.capture());
    assertEquals(RuntimeException.class.getName(), failure.getValue().getMessage());
    assertTrue(failure.getValue().getStackTrace().length > 0);
    skipThread.interrupt();
  }

  @Test
  void poll_whenDeadlineSpentSkipping_thenReturnsEmptyWithoutFurtherPolling() throws Exception {
    when(consumer.poll(any())).thenThrow(deserializationException(5L, new RuntimeException("bad")));
    KafkaWorker<byte[]> skipWorker = worker(OnError.SKIP);
    Thread skipThread = new Thread(skipWorker);
    skipThread.start();

    List<ConsumerRecord<String, byte[]>> got = skipWorker.poll(0);

    verify(consumer, times(1)).poll(any());
    verify(consumer).seek(new TopicPartition(TOPIC, 1), 6L);
    assertEquals(List.of(), got);
    skipThread.interrupt();
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
