package io.numaproj.kafka.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.numaproj.kafka.config.UserConfig;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class WorkerTest {
  private final UserConfig userConfigMock = mock(UserConfig.class);
  private final KafkaConsumer<String, GenericRecord> consumer = mock(KafkaConsumer.class);

  private static final String TEST_TOPIC = "test-topic";

  private Worker underTest;

  @BeforeEach
  public void setUp() {
    underTest = new Worker(userConfigMock, consumer);
    when(userConfigMock.getTopicName()).thenReturn(TEST_TOPIC);
    doNothing().when(consumer).subscribe(Collections.singleton(TEST_TOPIC));
  }

  @Test
  void run_success_consumeFromKafka() {
    Thread thread = new Thread(underTest);
    try {
      ConsumerRecords<String, GenericRecord> consumerRecords =
          generateConsumerRecords(TEST_TOPIC, 2);
      doAnswer(mi -> consumerRecords).when(consumer).poll(any());
      thread.start();
      List<ConsumerRecord<String, GenericRecord>> got = underTest.poll();
      List<ConsumerRecord<String, GenericRecord>> want = new ArrayList<>();
      for (ConsumerRecord<String, GenericRecord> record : consumerRecords) {
        want.add(record);
      }
      assertEquals(want, got);
    } catch (Exception e) {
      fail();
    }
    thread.interrupt();
  }

  @Test
  void run_success_consumeFromKafka_ignoreNullRecords() {
    Thread thread = new Thread(underTest);
    try {
      ConsumerRecords<String, GenericRecord> consumerRecords =
          generateConsumerRecords(TEST_TOPIC, 1, true);
      doAnswer(mi -> consumerRecords).when(consumer).poll(any());
      thread.start();
      var got = underTest.poll();
      assertTrue(got.isEmpty());
    } catch (Exception e) {
      fail();
    }
    thread.interrupt();
  }

  @Test
  void run_success_commitOffset() {
    Thread thread = new Thread(underTest);
    try {
      thread.start();
      underTest.commit();
      verify(consumer, times(1)).commitAsync(ArgumentMatchers.any(OffsetCommitCallback.class));
    } catch (Exception e) {
      fail();
    }
    thread.interrupt();
  }

  @Test
  void getPartitions() {
    try (var ignored = Mockito.mockConstruction(CountDownLatch.class)) {
      Set<TopicPartition> topicPartitions =
          new HashSet<>(
              Arrays.asList(
                  new TopicPartition(TEST_TOPIC, 1),
                  new TopicPartition(TEST_TOPIC, 3),
                  new TopicPartition(TEST_TOPIC, 6)));
      when(consumer.assignment()).thenReturn(topicPartitions);
      List<Integer> partitions = underTest.getPartitions();
      assertEquals(new HashSet<>(Arrays.asList(1, 3, 6)), new HashSet<>(partitions));
    }
  }

  private ConsumerRecords<String, GenericRecord> generateConsumerRecords(
      String topic, int numberOfRecords) {
    return generateConsumerRecords(topic, numberOfRecords, false);
  }

  private ConsumerRecords<String, GenericRecord> generateConsumerRecords(
      String topic, int numberOfRecords, boolean generateNullRecord) {
    Map<TopicPartition, List<ConsumerRecord<String, GenericRecord>>> records =
        new LinkedHashMap<>();
    List<ConsumerRecord<String, GenericRecord>> consumerRecordList = new ArrayList<>();
    for (int i = 0; i < numberOfRecords; i++) {
      String testSchema =
          "{"
              + "\"type\": \"record\","
              + "\"name\": \"User\","
              + "\"fields\": ["
              + "  { \"name\": \"name\", \"type\": \"string\" }"
              + "]"
              + "}";
      var schema = new Schema.Parser().parse(testSchema);
      var value = new GenericData.Record(schema);
      if (generateNullRecord) value = null;
      ConsumerRecord<String, GenericRecord> record1 =
          new ConsumerRecord<>(
              topic, 1, i, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, "k1" + i, value);
      consumerRecordList.add(record1);
    }
    records.put(new TopicPartition(topic, 1), consumerRecordList);
    return new ConsumerRecords<>(records);
  }
}
