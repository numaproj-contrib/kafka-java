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
  // Sorts after TOPIC, so its partition IDs start above the range reserved for TOPIC.
  private static final String OTHER_TOPIC = "z-other-topic";

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
    return new KafkaWorker<>(userConfig(onError, List.of(TOPIC)), consumer, handler, null);
  }

  /** A worker over two topics, with the partition ID map the source would build for them. */
  private KafkaWorker<byte[]> multiTopicWorker() {
    return new KafkaWorker<>(
        userConfig(OnError.FAIL, List.of(TOPIC, OTHER_TOPIC)),
        consumer,
        new SkippedRecordHandler(metrics),
        PartitionIdMapper.of(Map.of(TOPIC, 4, OTHER_TOPIC, 2)));
  }

  private static UserConfig userConfig(OnError onError, List<String> topicNames) {
    UserConfig userConfig = mock(UserConfig.class);
    when(userConfig.getTopicNames()).thenReturn(topicNames);
    when(userConfig.getOnError()).thenReturn(onError);
    return userConfig;
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
    verify(metrics, never()).recordSkipped(any());
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
    verify(metrics).recordSkipped(TOPIC);
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
    verify(metrics, times(2)).recordSkipped(TOPIC);
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
  void run_singleTopic_thenSubscribesToThatTopic() throws Exception {
    when(consumer.poll(any())).thenReturn(records("a"));
    thread.start();
    worker.poll(1000);

    assertEquals(List.of(TOPIC), captureSubscribedTopics());
  }

  @Test
  void run_multiTopic_thenSubscribesToEveryConfiguredTopic() throws Exception {
    // One consumer over all the topics, so they share a group and merge into one stream.
    when(consumer.poll(any())).thenReturn(records("a"));
    KafkaWorker<byte[]> multiTopicWorker = multiTopicWorker();
    Thread multiTopicThread = new Thread(multiTopicWorker);
    multiTopicThread.start();
    multiTopicWorker.poll(1000);

    assertEquals(Set.of(TOPIC, OTHER_TOPIC), new HashSet<>(captureSubscribedTopics()));
    multiTopicThread.interrupt();
  }

  @SuppressWarnings("unchecked")
  private Collection<String> captureSubscribedTopics() {
    ArgumentCaptor<Collection<String>> topics = ArgumentCaptor.forClass(Collection.class);
    verify(consumer).subscribe(topics.capture());
    return topics.getValue();
  }

  @Test
  void commit_delegatesToConsumer() throws Exception {
    thread.start();
    worker.commit();
    verify(consumer).commitAsync(any(OffsetCommitCallback.class));
  }

  @Test
  void getPartitions_singleTopic_thenReturnsTheAssignedKafkaPartitionNumbers() {
    // The upgrade guarantee: an existing deployment keeps the IDs its watermark entities are named
    // after, and a topic it does not consume is filtered out.
    assign(new TopicPartition(TOPIC, 1), new TopicPartition(TOPIC, 3), new TopicPartition("other", 9));

    assertEquals(Set.of(1, 3), new HashSet<>(pollThenGetPartitions(worker)));
  }

  @Test
  void getPartitions_multiTopic_thenReturnsGlobalIdsFromEachTopicsRange() {
    // test-topic sorts before z-other-topic, so it takes 0-3 and z-other-topic starts at 4.
    KafkaWorker<byte[]> multiTopicWorker = multiTopicWorker();
    assign(
        new TopicPartition(TOPIC, 0),
        new TopicPartition(TOPIC, 3),
        new TopicPartition(OTHER_TOPIC, 0),
        new TopicPartition(OTHER_TOPIC, 1));

    assertEquals(Set.of(0, 3, 4, 5), new HashSet<>(pollThenGetPartitions(multiTopicWorker)));
  }

  @Test
  void getPartitions_multiTopicWithAnUnconfiguredTopicAssigned_thenDropsItRatherThanThrowing() {
    KafkaWorker<byte[]> multiTopicWorker = multiTopicWorker();
    assign(new TopicPartition(TOPIC, 0), new TopicPartition("never-configured", 0));

    assertEquals(List.of(0), pollThenGetPartitions(multiTopicWorker));
  }

  @Test
  void getPartitions_beforeTheFirstPoll_thenIsEmpty() {
    // Legitimately empty until the worker has polled once; there is no safe value to invent, since
    // the replica index Sourcer.defaultPartitions() returns is a real ID under multi-topic.
    assign(new TopicPartition(TOPIC, 1));

    assertEquals(List.of(), worker.getPartitions());
  }

  @Test
  void getPartitions_afterARebalance_thenTheNextPollRefreshesIt() throws Exception {
    when(consumer.poll(any())).thenReturn(records("a"));
    when(consumer.assignment())
        .thenReturn(Set.of(new TopicPartition(TOPIC, 1)))
        .thenReturn(Set.of(new TopicPartition(TOPIC, 1), new TopicPartition(TOPIC, 2)));
    thread.start();

    worker.poll(1000);
    assertEquals(List.of(1), worker.getPartitions());

    worker.poll(1000);
    assertEquals(List.of(1, 2), worker.getPartitions());
  }

  @Test
  void getPartitions_whilePolling_thenReadsTheSnapshotAndNeverTheConsumer() throws Exception {
    // KafkaConsumer is single-threaded; reading assignment() off the worker thread would risk a
    // ConcurrentModificationException, so getPartitions() must not touch the consumer at all.
    when(consumer.poll(any())).thenReturn(records("a"));
    assign(new TopicPartition(TOPIC, 1));
    thread.start();
    worker.poll(1000);
    clearInvocations(consumer);

    worker.getPartitions();

    verify(consumer, never()).assignment();
  }

  /** Runs one poll so the worker snapshots the assignment, then reads the partitions back. */
  private List<Integer> pollThenGetPartitions(KafkaWorker<byte[]> underTest) {
    when(consumer.poll(any())).thenReturn(records("a"));
    Thread workerThread = new Thread(underTest);
    workerThread.start();
    try {
      underTest.poll(1000);
      return underTest.getPartitions();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      workerThread.interrupt();
    }
  }

  private void assign(TopicPartition... topicPartitions) {
    when(consumer.assignment()).thenReturn(Set.of(topicPartitions));
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
