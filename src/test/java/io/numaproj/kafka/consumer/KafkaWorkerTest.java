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
import org.mockito.InOrder;

/** The worker is format agnostic, so byte[] values are used to exercise its behavior. */
class KafkaWorkerTest {

  private static final String TOPIC = "test-topic";

  @SuppressWarnings("unchecked")
  private final KafkaConsumer<String, byte[]> consumer = mock(KafkaConsumer.class);

  private final SourceMetrics metrics = mock(SourceMetrics.class);
  private final SkippedRecordSink sink = mock(SkippedRecordSink.class);

  private KafkaWorker<byte[]> worker;
  private Thread thread;

  @BeforeEach
  void setUp() {
    worker = newWorker(OnError.FAIL);
    thread = new Thread(worker);
  }

  private KafkaWorker<byte[]> newWorker(OnError onError) {
    return new KafkaWorker<>(
        mockUserConfig(onError), consumer, new SkippedRecordHandler(metrics, sink));
  }

  private static UserConfig mockUserConfig(OnError onError) {
    UserConfig userConfig = mock(UserConfig.class);
    when(userConfig.getTopicName()).thenReturn(TOPIC);
    when(userConfig.getOnError()).thenReturn(onError);
    return userConfig;
  }

  /** Not the {@code @Deprecated} 4-arg constructor - see KafkaWorker's pollRecords javadoc. */
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
  void poll_returnsRecordsAndSkipsNullValues() throws Exception {
    when(consumer.poll(any())).thenReturn(records("a", null, "b"));
    thread.start();

    List<ConsumerRecord<String, byte[]>> got = worker.poll(1000);

    assertEquals(2, got.size());
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
  void poll_onErrorFail_propagatesDeserializationExceptionAndNeverSeeks() throws Exception {
    RecordDeserializationException deserializationException =
        deserializationException(5L, new RuntimeException("bad avro"));
    when(consumer.poll(any())).thenThrow(deserializationException);
    thread.start();

    RecordDeserializationException thrown =
        assertThrows(RecordDeserializationException.class, () -> worker.poll(1000));

    assertSame(deserializationException, thrown);
    verify(consumer, never()).seek(any(), anyLong());
    verifyNoInteractions(sink);
  }

  @Test
  void poll_onErrorSkip_seeksPastBadRecordAndStopsRetryingOnceDeadlineElapses() throws Exception {
    worker = newWorker(OnError.SKIP);
    thread = new Thread(worker);
    RecordDeserializationException deserializationException =
        deserializationException(5L, new RuntimeException("bad avro"));
    when(consumer.poll(any())).thenThrow(deserializationException);
    thread.start();

    List<ConsumerRecord<String, byte[]>> got = worker.poll(0);

    assertEquals(List.of(), got);
    verify(consumer, times(1)).poll(any());
    verify(consumer, times(1)).seek(new TopicPartition(TOPIC, 1), 6L);
  }

  @Test
  void poll_onErrorSkip_seeksPastMultipleConsecutiveBadRecords() throws Exception {
    worker = newWorker(OnError.SKIP);
    thread = new Thread(worker);
    RecordDeserializationException bad5 =
        deserializationException(5L, new RuntimeException("bad record 1"));
    RecordDeserializationException bad6 =
        deserializationException(6L, new RuntimeException("bad record 2"));
    when(consumer.poll(any())).thenThrow(bad5).thenThrow(bad6).thenReturn(records("good"));
    thread.start();

    List<ConsumerRecord<String, byte[]>> got = worker.poll(1000);

    assertEquals(1, got.size());
    InOrder order = inOrder(consumer);
    order.verify(consumer).seek(new TopicPartition(TOPIC, 1), 6L);
    order.verify(consumer).seek(new TopicPartition(TOPIC, 1), 7L);
  }

  @Test
  void poll_nullValueRecords_areDroppedAndCountedAsTombstones() throws Exception {
    when(consumer.poll(any())).thenReturn(records("good", null, "also-good"));
    thread.start();

    List<ConsumerRecord<String, byte[]>> got = worker.poll(1000);

    assertEquals(2, got.size());
    verify(metrics).recordDropped(SourceMetrics.DropReason.NULL_VALUE);
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
