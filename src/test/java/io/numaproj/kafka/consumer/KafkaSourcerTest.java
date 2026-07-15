package io.numaproj.kafka.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.numaproj.kafka.format.ByteArrayFormat;
import io.numaproj.numaflow.sourcer.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class KafkaSourcerTest {

  private static final String TOPIC = "test-topic";

  private final Admin admin = mock(Admin.class);
  @SuppressWarnings("unchecked")
  private final KafkaWorker<byte[]> worker = mock(KafkaWorker.class);
  private final OutputObserver observer = mock(OutputObserver.class);

  private KafkaSourcer<byte[]> underTest;

  @BeforeEach
  void setUp() {
    underTest =
        Mockito.spy(new KafkaSourcer<>(null, admin, new ByteArrayFormat(), batchSize -> null));
    Thread aliveThread = mock(Thread.class);
    when(aliveThread.isAlive()).thenReturn(true);
    underTest.setWorker(worker, aliveThread);
  }

  private static ReadRequest readRequest(long count) {
    ReadRequest request = mock(ReadRequest.class);
    when(request.getCount()).thenReturn(count);
    when(request.getTimeout()).thenReturn(Duration.ofMillis(100));
    return request;
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
}
