package io.numaproj.kafka.consumer;

import static io.numaproj.kafka.consumer.Utils.generateTestData;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

import io.numaproj.numaflow.sourcer.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class AvroSourcerTest {
  private final Admin adminMock = mock(Admin.class);
  private final AvroWorker avroWorkerMock = mock(AvroWorker.class);

  private AvroSourcer underTest;

  @Test
  void givenSourcer_whenStartConsumer_thenNumaflowSourceServerStarted() {
    try {
      underTest = Mockito.spy(new AvroSourcer(avroWorkerMock, adminMock));
      try (var mockedConstruction = Mockito.mockConstruction(Server.class)) {
        AtomicBoolean runMethodInvoked = new AtomicBoolean(false);
        doAnswer(
                methodInvocation -> {
                  runMethodInvoked.set(true);
                  return null;
                })
            .when(avroWorkerMock)
            .run();
        underTest.startConsumer();
        verify(mockedConstruction.constructed().getFirst(), times(1)).start();
        Thread.sleep(100);
        assertTrue(runMethodInvoked.get());
      }
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  void givenSourcer_whenOneMessageAvailable_thenOneMessageSentByObserver() {
    try {
      ReadRequest readRequest = mock(ReadRequest.class);
      when(readRequest.getCount()).thenReturn(1L);
      when(readRequest.getTimeout()).thenReturn(Duration.ofMillis(100));
      List<ConsumerRecord<String, GenericRecord>> consumerRecords = new ArrayList<>(10);
      ConsumerRecord<String, GenericRecord> consumerRecord =
          new ConsumerRecord<>("foo", 1, 1, "bar", generateTestData());
      consumerRecords.add(consumerRecord);
      OutputObserver outputObserver = Mockito.mock(OutputObserver.class);
      underTest = Mockito.spy(new AvroSourcer(avroWorkerMock, adminMock));
      Mockito.doReturn(true).when(underTest).isWorkerThreadAlive();
      doReturn(consumerRecords).when(avroWorkerMock).poll();
      doAnswer(
              methodInvocation -> {
                methodInvocation.getArgument(0);
                return null;
              })
          .when(outputObserver)
          .send(any());
      underTest.read(readRequest, outputObserver);
      verify(outputObserver, times(1)).send(any(Message.class));
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  void givenSourcer_whenReadThrowsThreadInterrupted_thenSourcerKilledAndReadTimeoutIsRespected() {
    try {
      ReadRequest readRequest = mock(ReadRequest.class);
      when(readRequest.getCount()).thenReturn(1L);
      when(readRequest.getTimeout()).thenReturn(Duration.ofMillis(100));
      OutputObserver outputObserver = Mockito.mock(OutputObserver.class);

      underTest = Mockito.spy(new AvroSourcer(avroWorkerMock, adminMock));
      Mockito.doReturn(true).when(underTest).isWorkerThreadAlive();
      doThrow(new InterruptedException("foo")).when(avroWorkerMock).poll();
      doAnswer(
              mi -> {
                // more than read request time out
                Thread.sleep(200);
                return null;
              })
          .when(underTest)
          .kill(any(RuntimeException.class));

      underTest.read(readRequest, outputObserver);

      verify(outputObserver, times(0)).send(any(Message.class));
      verify(underTest, times(1)).kill(any(RuntimeException.class));
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  void givenSourcer_whenCustomHeadersPresent_thenCustomHeadersKept() {
    try {
      ReadRequest readRequest = mock(ReadRequest.class);
      when(readRequest.getCount()).thenReturn(1L);
      when(readRequest.getTimeout()).thenReturn(Duration.ofMillis(100));
      Headers headers = new RecordHeaders();
      headers.add("foo", "bar".getBytes());
      List<ConsumerRecord<String, GenericRecord>> consumerRecords = new ArrayList<>(10);
      var cr = new ConsumerRecord<>("foo", 1, 1, "bar", generateTestData());
      cr.headers().add("foo", "bar".getBytes());
      consumerRecords.add(cr);
      OutputObserver outputObserver = Mockito.mock(OutputObserver.class);
      underTest = Mockito.spy(new AvroSourcer(avroWorkerMock, adminMock));
      Mockito.doReturn(true).when(underTest).isWorkerThreadAlive();
      doReturn(consumerRecords).when(avroWorkerMock).poll();
      doAnswer(
              methodInvocation -> {
                Message message = methodInvocation.getArgument(0);
                assertEquals(
                    "bar", message.getHeaders().get("foo"), "expected custom header to be set");
                return null;
              })
          .when(outputObserver)
          .send(any());
      underTest.read(readRequest, outputObserver);
      verify(outputObserver, times(1)).send(any(Message.class));
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  void givenSourcerRead_whenWorkerThreadIsNotAlive_thenKillInvoked() {
    ReadRequest readRequest = mock(ReadRequest.class);
    when(readRequest.getCount()).thenReturn(1L);
    underTest = Mockito.spy(new AvroSourcer(avroWorkerMock, adminMock));
    doReturn(false).when(underTest).isWorkerThreadAlive();
    doAnswer(
            mi -> {
              // more than read request time out
              Thread.sleep(200);
              return null;
            })
        .when(underTest)
        .kill(any(RuntimeException.class));
    underTest.read(readRequest, null);
    verify(underTest, times(1)).kill(any(RuntimeException.class));
  }

  @Test
  void givenSourcerRead_whenReadTimeout_thenDoNotReadMore() {
    try {
      ReadRequest readRequest = mock(ReadRequest.class);
      underTest = Mockito.spy(new AvroSourcer(avroWorkerMock, adminMock));

      when(readRequest.getCount()).thenReturn(2L);
      // set timeout to 100 milliseconds
      when(readRequest.getTimeout()).thenReturn(Duration.ofMillis(100));
      OutputObserver outputObserver = Mockito.mock(OutputObserver.class);
      doReturn(true).when(underTest).isWorkerThreadAlive();

      doAnswer(
              methodInvocation -> {
                // intentionally introduce delay in polling
                Thread.sleep(1000);
                List<ConsumerRecord<String, GenericRecord>> consumerRecords = new ArrayList<>(10);
                ConsumerRecord<String, GenericRecord> consumerRecord =
                    new ConsumerRecord<>("foo", 1, 1, "bar", generateTestData());
                consumerRecord.headers().add("eb_cluster_id", "abc".getBytes());
                consumerRecords.add(consumerRecord);
                return consumerRecords;
              })
          .when(avroWorkerMock)
          .poll();
      underTest.read(readRequest, outputObserver);
      // verify that only one message is read
      verify(outputObserver, times(1)).send(any());
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  void givenSourcerRead_whenConsumerRecordIsNull_thenSkipTheRecord() {
    try {
      ReadRequest readRequest = mock(ReadRequest.class);
      when(readRequest.getCount()).thenReturn(1L);
      when(readRequest.getTimeout()).thenReturn(Duration.ofMillis(10));
      List<ConsumerRecord<String, GenericRecord>> consumerRecords = new ArrayList<>();
      consumerRecords.add(null);
      OutputObserver outputObserver = Mockito.mock(OutputObserver.class);
      underTest = Mockito.spy(new AvroSourcer(avroWorkerMock, adminMock));
      doReturn(true).when(underTest).isWorkerThreadAlive();
      doReturn(consumerRecords).when(avroWorkerMock).poll();
      underTest.read(readRequest, outputObserver);
      verify(outputObserver, times(0)).send(any());
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  void givenSourcerRead_whenConsumerRecordsAreNull_thenSkipTheRecords() {
    try {
      ReadRequest readRequest = mock(ReadRequest.class);
      when(readRequest.getCount()).thenReturn(1L);
      when(readRequest.getTimeout()).thenReturn(Duration.ofMillis(100));
      OutputObserver outputObserver = Mockito.mock(OutputObserver.class);
      underTest = Mockito.spy(new AvroSourcer(avroWorkerMock, adminMock));
      doReturn(true).when(underTest).isWorkerThreadAlive();
      doReturn(null).when(avroWorkerMock).poll();
      underTest.read(readRequest, outputObserver);
      verify(outputObserver, times(0)).send(any());
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  void givenSourcerAck_whenAckRequestReceived_thenWorkerCommit() {
    try {
      underTest = Mockito.spy(new AvroSourcer(avroWorkerMock, adminMock));
      String offsetValue = "test-topic" + ":" + 1;
      Offset offset = new Offset(offsetValue.getBytes(StandardCharsets.UTF_8), 10);
      List<Offset> offsets = new ArrayList<>();
      offsets.add(offset);
      var ackRequest =
          new AckRequest() {
            @Override
            public List<Offset> getOffsets() {
              return offsets;
            }
          };
      underTest.ack(ackRequest);
      verify(avroWorkerMock, times(1)).commit();
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  void givenSourcerAck_whenRequestOffsetIsOutOfSyncWithPreviousRead_thenErrorMetricsEmitted() {
    try {
      Map<String, Long> readTopicPartionMap = new HashMap<>();
      readTopicPartionMap.put("test-topic:10", 100L);
      underTest = Mockito.spy(new AvroSourcer(avroWorkerMock, adminMock));
      underTest.setReadTopicPartitionOffsetMap(readTopicPartionMap);
      // the requested offset doesn't exist in readTopicPartitionOffsetMap, which indicates that
      // the ack is out of sync with the previous read
      String offsetValue = "test-topic" + ":" + 1;
      Offset offset = new Offset(offsetValue.getBytes(StandardCharsets.UTF_8), 10);
      List<Offset> offsets = new ArrayList<>();
      offsets.add(offset);
      var ackRequest =
          new AckRequest() {
            @Override
            public List<Offset> getOffsets() {
              return offsets;
            }
          };
      underTest.ack(ackRequest);
      // TODO - verify that error metrics are emitted, once we have metrics added.
      verify(avroWorkerMock, times(1)).commit();
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  void givenSourcerAck_whenAckInterrupted_sourcerKilled() {
    try {
      underTest = Mockito.spy(new AvroSourcer(avroWorkerMock, adminMock));
      doThrow(new InterruptedException("foo")).when(avroWorkerMock).commit();
      String offsetValue = "test-topic" + ":" + 1;
      Offset offset = new Offset(offsetValue.getBytes(StandardCharsets.UTF_8), 10);
      List<Offset> offsets = new ArrayList<>();
      offsets.add(offset);
      var ackRequest =
          new AckRequest() {
            @Override
            public List<Offset> getOffsets() {
              return offsets;
            }
          };
      doNothing().when(underTest).kill(any(RuntimeException.class));
      underTest.ack(ackRequest);
      verify(avroWorkerMock, times(1)).commit();
      verify(underTest, times(1)).kill(any(RuntimeException.class));
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  void givenSourcer_whenGetPending_thenAdminReturnPendingCount() {
    underTest = Mockito.spy(new AvroSourcer(avroWorkerMock, adminMock));
    doReturn(100L).when(adminMock).getPendingMessages();
    long count = underTest.getPending();
    assertEquals(100L, count);
  }

  @Test
  void givenSourcer_whenGetPartitions_thenWorkerReturnPartitions() {
    underTest = Mockito.spy(new AvroSourcer(avroWorkerMock, adminMock));
    doReturn(List.of(1)).when(avroWorkerMock).getPartitions();
    underTest.getPartitions();
    verify(avroWorkerMock, times(1)).getPartitions();
    assertEquals(1, underTest.getPartitions().size());
    assertEquals(1, underTest.getPartitions().getFirst());
  }
}
