package io.numaproj.kafka.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.numaproj.kafka.config.OnError;
import io.numaproj.kafka.config.UserConfig;
import io.numaproj.kafka.format.ByteArrayFormat;
import io.numaproj.kafka.format.FormatException;
import io.numaproj.kafka.format.KafkaFormat;
import io.numaproj.kafka.metrics.SourceMetrics;
import io.numaproj.numaflow.sourcer.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;

class KafkaSourcerTest {

  private static final String TOPIC = "test-topic";

  // Hard-coded on purpose, and NOT read from KafkaSourcer.KAFKA_TOPIC_HEADER: the header key is a
  // wire contract shared with Numaflow's built-in Kafka source and with downstream vertices already
  // reading it. Changing the constant must fail this test rather than silently pass.
  private static final String TOPIC_HEADER = "X-NF-Kafka-TopicName";

  private final Admin admin = mock(Admin.class);
  private final SourceMetrics metrics = mock(SourceMetrics.class);
  @SuppressWarnings("unchecked")
  private final KafkaWorker<byte[]> worker = mock(KafkaWorker.class);
  private final OutputObserver observer = mock(OutputObserver.class);

  private KafkaSourcer<byte[]> underTest;

  @BeforeEach
  void setUp() {
    underTest = sourcer(new ByteArrayFormat(), OnError.FAIL, worker);
  }

  /** A mocked UserConfig, stubbed only with the {@code onError} the sourcer reads on a failure. */
  private static UserConfig userConfig(OnError onError) {
    UserConfig userConfig = mock(UserConfig.class);
    when(userConfig.getOnError()).thenReturn(onError);
    return userConfig;
  }

  private KafkaSourcer<byte[]> sourcer(
      KafkaFormat<byte[]> format, OnError onError, KafkaWorker<byte[]> worker) {
    KafkaSourcer<byte[]> sourcer =
        Mockito.spy(
            new KafkaSourcer<>(userConfig(onError), admin, format, batchSize -> null, metrics));
    Thread aliveThread = mock(Thread.class);
    when(aliveThread.isAlive()).thenReturn(true);
    sourcer.setWorker(worker, aliveThread);
    return sourcer;
  }

  private static ReadRequest readRequest(long count) {
    return readRequest(count, 100);
  }

  private static ReadRequest readRequest(long count, long timeoutMs) {
    ReadRequest request = mock(ReadRequest.class);
    when(request.getCount()).thenReturn(count);
    when(request.getTimeout()).thenReturn(Duration.ofMillis(timeoutMs));
    return request;
  }

  private static RecordDeserializationException rde(long offset) {
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
        new RuntimeException("bad avro"));
  }

  private static ConsumerRecord<String, byte[]> tombstone(long offset) {
    return new ConsumerRecord<>(TOPIC, 1, offset, "key", null);
  }

  private static ConsumerRecord<String, byte[]> record(long offset) {
    return new ConsumerRecord<>(TOPIC, 1, offset, "key", "value".getBytes());
  }

  @Test
  void read_sendsOneMessagePerRecord() throws Exception {
    when(worker.poll(anyLong())).thenReturn(List.of(record(1)));
    underTest.read(readRequest(1), observer);
    verify(observer, times(1)).send(any(Message.class));
  }

  @Test
  void read_keepsCustomHeaders() throws Exception {
    ConsumerRecord<String, byte[]> record = record(1);
    record.headers().add("foo", "bar".getBytes());
    when(worker.poll(anyLong())).thenReturn(List.of(record));

    underTest.read(readRequest(1), observer);

    verify(observer)
        .send(argThat(message -> "bar".equals(message.getHeaders().get("foo"))));
  }

  @Test
  void read_setsTopicHeader() throws Exception {
    when(worker.poll(anyLong())).thenReturn(List.of(record(1)));

    underTest.read(readRequest(1), observer);

    verify(observer)
        .send(argThat(message -> TOPIC.equals(message.getHeaders().get(TOPIC_HEADER))));
  }

  @Test
  void read_whenRecordCarriesTopicHeader_thenActualTopicWins() throws Exception {
    ConsumerRecord<String, byte[]> record = record(1);
    record.headers().add(TOPIC_HEADER, "spoofed-topic".getBytes());
    when(worker.poll(anyLong())).thenReturn(List.of(record));

    underTest.read(readRequest(1), observer);

    verify(observer)
        .send(argThat(message -> TOPIC.equals(message.getHeaders().get(TOPIC_HEADER))));
  }

  @Test
  void read_skipsNullRecordsAndNullList() throws Exception {
    when(worker.poll(anyLong())).thenReturn(java.util.Collections.singletonList(null));
    underTest.read(readRequest(1), observer);

    when(worker.poll(anyLong())).thenReturn(null);
    underTest.read(readRequest(1), observer);

    verify(observer, never()).send(any());
  }

  @Test
  void read_whenPollInterrupted_thenKills() throws Exception {
    when(worker.poll(anyLong())).thenThrow(new InterruptedException("boom"));
    doNothing().when(underTest).kill(any());
    underTest.read(readRequest(1), observer);
    verify(underTest).kill(any(RuntimeException.class));
  }

  @Test
  void read_whenFormatFailsAndOnErrorFail_thenKillsWithFailureIdentifyingRecordByCoordinatesOnly()
      throws Exception {
    KafkaSourcer<byte[]> sourcer = sourcer(failingFormat(), OnError.FAIL, worker);
    ConsumerRecord<String, byte[]> sensitiveRecord =
        new ConsumerRecord<>(TOPIC, 1, 42L, "key", "super-secret-value".getBytes());
    when(worker.poll(anyLong())).thenReturn(List.of(sensitiveRecord));
    doNothing().when(sourcer).kill(any());

    sourcer.read(readRequest(1), observer);

    verify(observer, never()).send(any());
    ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
    verify(sourcer).kill(captor.capture());
    String message = captor.getValue().getMessage();
    assertTrue(message.contains("offset:42"));
    assertFalse(message.contains("ConsumerRecord"));
    assertFalse(message.contains("super-secret-value"));
  }

  @Test
  void read_whenFormatFailsAndOnErrorSkip_thenDropsWithoutSendingOrKilling() throws Exception {
    KafkaSourcer<byte[]> sourcer = sourcer(failingFormat(), OnError.SKIP, worker);
    when(worker.poll(anyLong())).thenReturn(List.of(record(7)));
    doNothing().when(sourcer).kill(any());

    sourcer.read(readRequest(1), observer);

    verify(observer, never()).send(any());
    verify(sourcer, never()).kill(any());
  }

  @SuppressWarnings("unchecked")
  private static KafkaFormat<byte[]> failingFormat() {
    KafkaFormat<byte[]> format = mock(KafkaFormat.class);
    try {
      when(format.toPayload(any())).thenThrow(new FormatException("boom"));
    } catch (FormatException e) {
      throw new RuntimeException(e);
    }
    return format;
  }

  @Test
  void read_whenRecordCannotBeDeserializedAndOnErrorSkip_thenCountsSeeksPastAndKeepsReading()
      throws Exception {
    KafkaSourcer<byte[]> sourcer = sourcer(new ByteArrayFormat(), OnError.SKIP, worker);
    when(worker.poll(anyLong())).thenThrow(rde(6)).thenReturn(List.of(record(7)));
    doNothing().when(sourcer).kill(any());

    sourcer.read(readRequest(2), observer);

    verify(metrics).recordSkipped();
    verify(worker).seekPast(new RecordLocation(TOPIC, 1, 6L));
    verify(observer, times(1)).send(any(Message.class));
    verify(sourcer, never()).kill(any());
  }

  @Test
  void read_whenConsecutiveRecordsCannotBeDeserialized_thenSeeksPastEachInTurn() throws Exception {
    KafkaSourcer<byte[]> sourcer = sourcer(new ByteArrayFormat(), OnError.SKIP, worker);
    when(worker.poll(anyLong()))
        .thenThrow(rde(5))
        .thenThrow(rde(6))
        .thenReturn(List.of(record(7)));

    sourcer.read(readRequest(3), observer);

    InOrder order = inOrder(worker);
    order.verify(worker).seekPast(new RecordLocation(TOPIC, 1, 5L));
    order.verify(worker).seekPast(new RecordLocation(TOPIC, 1, 6L));
    verify(metrics, times(2)).recordSkipped();
    verify(observer, times(1)).send(any(Message.class));
  }

  @Test
  void read_whenTheDeadlineIsSpentSkipping_thenForwardsNothingAndDoesNotPollAgain()
      throws Exception {
    KafkaSourcer<byte[]> sourcer = sourcer(new ByteArrayFormat(), OnError.SKIP, worker);
    when(worker.poll(anyLong())).thenThrow(rde(6));

    sourcer.read(readRequest(1, 0), observer);

    verify(worker, times(1)).poll(anyLong());
    verify(worker).seekPast(new RecordLocation(TOPIC, 1, 6L));
    verify(observer, never()).send(any());
  }

  @Test
  void read_whenRecordCannotBeDeserializedAndOnErrorFail_thenKillsWithoutSeeking() throws Exception {
    when(worker.poll(anyLong())).thenThrow(rde(6));
    doNothing().when(underTest).kill(any());

    underTest.read(readRequest(1), observer);

    verify(underTest).kill(any(RecordDeserializationException.class));
    verify(worker, never()).seekPast(any());
    verify(metrics, never()).recordSkipped();
  }

  @Test
  void read_tombstone_isCountedAndNotForwarded() throws Exception {
    when(worker.poll(anyLong())).thenReturn(List.of(record(5), tombstone(6), record(7)));

    underTest.read(readRequest(3), observer);

    verify(observer, times(2)).send(any(Message.class));
    verify(metrics, times(1)).recordSkipped();
  }

  @Test
  void read_whenPollThrowsRuntimeException_thenKills() throws Exception {
    when(worker.poll(anyLong())).thenThrow(new RuntimeException("boom"));
    doNothing().when(underTest).kill(any());
    underTest.read(readRequest(1), observer);
    verify(underTest).kill(any(RuntimeException.class));
  }

  @Test
  void read_whenWorkerThreadDead_thenKills() {
    Thread deadThread = mock(Thread.class);
    when(deadThread.isAlive()).thenReturn(false);
    underTest.setWorker(worker, deadThread);
    doNothing().when(underTest).kill(any());

    underTest.read(readRequest(1), observer);

    verify(underTest).kill(any(RuntimeException.class));
  }

  @Test
  void ack_commitsOffsets() throws Exception {
    underTest.ack(ackRequest());
    verify(worker).commit();
  }

  @Test
  void ack_whenOutOfSyncWithRead_stillCommits() throws Exception {
    underTest.setReadTopicPartitionOffsetMap(Map.of("test-topic:10", 100L));
    underTest.ack(ackRequest());
    verify(worker).commit();
  }

  @Test
  void ack_whenCommitInterrupted_thenKills() throws Exception {
    doThrow(new InterruptedException("boom")).when(worker).commit();
    doNothing().when(underTest).kill(any());
    underTest.ack(ackRequest());
    verify(underTest).kill(any(RuntimeException.class));
  }

  @Test
  void getPending_delegatesToAdmin() {
    when(admin.getPendingMessages()).thenReturn(100L);
    assertEquals(100L, underTest.getPending());
  }

  @Test
  void getPartitions_delegatesToWorker() {
    when(worker.getPartitions()).thenReturn(List.of(1));
    assertEquals(List.of(1), underTest.getPartitions());
  }

  private static AckRequest ackRequest() {
    Offset offset = new Offset((TOPIC + ":1").getBytes(StandardCharsets.UTF_8), 10);
    return new AckRequest() {
      @Override
      public List<Offset> getOffsets() {
        return List.of(offset);
      }
    };
  }

  private static AckRequest ackRequest(int partition, long... offsets) {
    List<Offset> acked = new ArrayList<>();
    for (long offset : offsets) {
      acked.add(
          new Offset((TOPIC + ":" + offset).getBytes(StandardCharsets.UTF_8), partition));
    }
    return () -> acked;
  }

  /** Fails to convert the record whose value is {@code "bad"}, and converts anything else. */
  @SuppressWarnings("unchecked")
  private static KafkaFormat<byte[]> formatFailingOnBadValue() {
    KafkaFormat<byte[]> format = mock(KafkaFormat.class);
    try {
      when(format.toPayload(any()))
          .thenAnswer(
              invocation -> {
                byte[] value = invocation.getArgument(0);
                if ("bad".equals(new String(value, StandardCharsets.UTF_8))) {
                  throw new FormatException("boom");
                }
                return value;
              });
    } catch (FormatException e) {
      throw new RuntimeException(e);
    }
    return format;
  }

  private static ConsumerRecord<String, byte[]> record(long offset, String value) {
    return new ConsumerRecord<>(TOPIC, 1, offset, "key", value.getBytes(StandardCharsets.UTF_8));
  }

  private static List<ILoggingEvent> captureErrors(Runnable action) {
    Logger logger = (Logger) LoggerFactory.getLogger(KafkaSourcer.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      action.run();
    } finally {
      logger.detachAppender(appender);
    }
    return appender.list.stream().filter(e -> e.getLevel() == Level.ERROR).toList();
  }

  @Test
  void read_whenRecordSkipped_thenAckOfTheForwardedOffsetStaysInSync() throws Exception {
    KafkaSourcer<byte[]> sourcer = sourcer(formatFailingOnBadValue(), OnError.SKIP, worker);
    // The skipped record sits at the higher offset, so tracking it would leave the read map ahead
    // of anything Numaflow can ack.
    when(worker.poll(anyLong())).thenReturn(List.of(record(5, "good"), record(6, "bad")));

    sourcer.read(readRequest(2), observer);

    verify(observer, times(1)).send(any(Message.class));
    // Numaflow acks only offset 5, the one record it received.
    List<ILoggingEvent> errors = captureErrors(() -> sourcer.ack(ackRequest(1, 5L)));
    assertEquals(List.of(), errors, "read and ack must agree when a record is skipped");
    verify(worker).commit();
  }

  @Test
  void ack_whenReadOffsetIsAheadOfTheAckedOffset_thenReportsOutOfSync() {
    // The state that tracking a skipped record would produce: read at offset 6, but Numaflow can
    // only ack offset 5, the last record it actually received.
    underTest.setReadTopicPartitionOffsetMap(new HashMap<>(Map.of(TOPIC + ":1", 6L)));

    List<ILoggingEvent> errors = captureErrors(() -> underTest.ack(ackRequest(1, 5L)));

    assertEquals(1, errors.size());
    assertTrue(errors.get(0).getFormattedMessage().contains("READ AND ACK ARE NOT IN SYNC"));
  }

  @Test
  void read_whenABatchIsFullySkipped_thenTheFollowingBatchAckCommitsPastIt() throws Exception {
    KafkaSourcer<byte[]> sourcer = sourcer(formatFailingOnBadValue(), OnError.SKIP, worker);

    // Nothing is forwarded, so no ack arrives: Numaflow only acks offsets it received, and so the
    // consumer position past offset 6 stays uncommitted for now.
    when(worker.poll(anyLong())).thenReturn(List.of(record(6, "bad")));
    sourcer.read(readRequest(1), observer);

    verify(observer, never()).send(any());
    verify(worker, never()).commit();

    // The next batch carries a readable record, and its ack commits the position - by then already
    // past offset 6.
    when(worker.poll(anyLong())).thenReturn(List.of(record(7, "good")));
    sourcer.read(readRequest(1), observer);

    List<ILoggingEvent> errors = captureErrors(() -> sourcer.ack(ackRequest(1, 7L)));
    assertEquals(List.of(), errors);
    verify(worker).commit();
  }
}
