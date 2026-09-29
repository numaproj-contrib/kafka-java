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
    when(userConfig.getTopics()).thenReturn(List.of(TOPIC));
    when(userConfig.getOnError()).thenReturn(onError);
    return new KafkaWorker<>(
        userConfig, consumer, handler, new PartitionIdMapper(List.of(TOPIC)));
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
  void poll_whenRecordCannotBeDeserializedAndOnErrorSkip_thenSeeksCountsAndReturnsEmpty()
      throws Exception {
    RuntimeException cause = new RuntimeException("bad avro");
    when(consumer.poll(any()))
        .thenThrow(deserializationException(5L, cause))
        .thenReturn(records("good"));
    KafkaWorker<byte[]> skipWorker = worker(OnError.SKIP);
    Thread skipThread = new Thread(skipWorker);
    skipThread.start();

    List<ConsumerRecord<String, byte[]>> skipped = skipWorker.poll(1000);
    List<ConsumerRecord<String, byte[]>> next = skipWorker.poll(1000);

    verify(consumer).seek(new TopicPartition(TOPIC, 1), 6L);
    verify(metrics).recordSkipped();
    assertEquals(List.of(), skipped);
    assertEquals(1, next.size());
    skipThread.interrupt();
  }

  @Test
  void poll_whenConsecutiveBadRecordsAndOnErrorSkip_thenSeeksPastOnePerPoll() throws Exception {
    when(consumer.poll(any()))
        .thenThrow(deserializationException(5L, new RuntimeException("bad")))
        .thenThrow(deserializationException(6L, new RuntimeException("bad")))
        .thenReturn(records("good"));
    KafkaWorker<byte[]> skipWorker = worker(OnError.SKIP);
    Thread skipThread = new Thread(skipWorker);
    skipThread.start();

    skipWorker.poll(1000);
    skipWorker.poll(1000);
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
  void poll_whenRecordSkipped_thenPollsOnlyOnce() throws Exception {
    // One poll per read: the skip is handed back as an empty batch instead of re-polling for
    // records within the same read.
    when(consumer.poll(any())).thenThrow(deserializationException(5L, new RuntimeException("bad")));
    KafkaWorker<byte[]> skipWorker = worker(OnError.SKIP);
    Thread skipThread = new Thread(skipWorker);
    skipThread.start();

    List<ConsumerRecord<String, byte[]>> got = skipWorker.poll(1000);

    verify(consumer, times(1)).poll(any());
    verify(consumer).seek(new TopicPartition(TOPIC, 1), 6L);
    assertEquals(List.of(), got);
    skipThread.interrupt();
  }

  @Test
  void poll_whenABatchEndsInABadRecord_thenTheGoodRecordsArriveBeforeTheSkip() throws Exception {
    // The consumer holds the failure back until it has no records left to return, so a batch of
    // [100, 101, 102, 103] whose last record is undeserializable arrives as [100, 101, 102]
    // followed by a throw for 103. The empty batch the skip hands back costs no good record.
    when(consumer.poll(any()))
        .thenReturn(records(100L, "a", "b", "c"))
        .thenThrow(deserializationException(103L, new RuntimeException("bad")))
        .thenReturn(records(104L, "d"));
    KafkaWorker<byte[]> skipWorker = worker(OnError.SKIP);
    Thread skipThread = new Thread(skipWorker);
    skipThread.start();

    List<ConsumerRecord<String, byte[]>> good = skipWorker.poll(1000);
    List<ConsumerRecord<String, byte[]>> skipped = skipWorker.poll(1000);
    List<ConsumerRecord<String, byte[]>> resumed = skipWorker.poll(1000);

    assertEquals(List.of(100L, 101L, 102L), good.stream().map(ConsumerRecord::offset).toList());
    assertEquals(List.of(), skipped);
    assertEquals(List.of(104L), resumed.stream().map(ConsumerRecord::offset).toList());
    verify(consumer).seek(new TopicPartition(TOPIC, 1), 104L);
    verify(metrics, times(1)).recordSkipped();
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
    // Single topic → topicIndex 0 → global ID equals the raw partition number.
    when(consumer.assignment())
        .thenReturn(
            Set.of(new TopicPartition(TOPIC, 1), new TopicPartition(TOPIC, 3),
                new TopicPartition("other", 9)));

    assertEquals(Set.of(1, 3), new HashSet<>(worker.getPartitions()));
  }

  @Test
  void run_subscribesToAllConfiguredTopics() throws Exception {
    UserConfig userConfig = mock(UserConfig.class);
    when(userConfig.getTopics()).thenReturn(List.of("topic-a", "topic-b"));
    KafkaWorker<byte[]> multiWorker =
        new KafkaWorker<>(
            userConfig,
            consumer,
            new SkippedRecordHandler(metrics),
            new PartitionIdMapper(List.of("topic-a", "topic-b")));
    Thread multiThread = new Thread(multiWorker);
    multiThread.start();

    // commit() blocks until the worker thread has processed a task, by which point run() has
    // already subscribed.
    multiWorker.commit();

    verify(consumer).subscribe(List.of("topic-a", "topic-b"));
    multiThread.interrupt();
  }

  @Test
  void getPartitions_acrossMultipleTopics_returnsNormalizedGlobalIds() {
    // Two topics both number partitions from 0; normalization maps them into disjoint blocks
    // (topic-a → 0.., topic-b → 256..) so they no longer collide.
    UserConfig userConfig = mock(UserConfig.class);
    when(userConfig.getTopics()).thenReturn(List.of("topic-a", "topic-b"));
    KafkaWorker<byte[]> multiWorker =
        new KafkaWorker<>(
            userConfig,
            consumer,
            new SkippedRecordHandler(metrics),
            new PartitionIdMapper(List.of("topic-a", "topic-b")));
    when(consumer.assignment())
        .thenReturn(
            Set.of(
                new TopicPartition("topic-a", 0),
                new TopicPartition("topic-a", 1),
                new TopicPartition("topic-b", 0),
                new TopicPartition("other", 9)));

    // topic-a/0 → 0, topic-a/1 → 1, topic-b/0 → 256; "other" is not configured and is excluded.
    assertEquals(Set.of(0, 1, 256), new HashSet<>(multiWorker.getPartitions()));
  }

  private static ConsumerRecords<String, byte[]> records(String... values) {
    return records(0L, values);
  }

  private static ConsumerRecords<String, byte[]> records(long firstOffset, String... values) {
    List<ConsumerRecord<String, byte[]>> list = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      byte[] value = values[i] == null ? null : values[i].getBytes();
      list.add(
          new ConsumerRecord<>(
              TOPIC, 1, firstOffset + i, 0L, TimestampType.CREATE_TIME, 0, 0, "k" + i, value,
              new RecordHeaders(), Optional.empty()));
    }
    return new ConsumerRecords<>(Map.of(new TopicPartition(TOPIC, 1), list));
  }
}
