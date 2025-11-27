package io.numaproj.kafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.numaproj.kafka.config.UserConfig;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AdminTest {
  private final UserConfig userConfigMock = mock(UserConfig.class);
  private final AdminClient adminClientMock = mock(AdminClient.class);

  private static final String TEST_TOPIC = "test-topic";
  private static final String TEST_GROUP_ID = "test-group-id";

  private Admin underTest;

  @BeforeEach
  public void setUp() {
    underTest = new Admin(userConfigMock, TEST_GROUP_ID, adminClientMock);
    when(userConfigMock.getTopicName()).thenReturn(TEST_TOPIC);
  }

  @Test
  public void getPendingMessages_success() {
    try {
      List<TopicPartition> topicPartitionList = generateTopicPartitions();
      Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap =
          generateTopicPartitionOffsetMetadata(topicPartitionList);
      Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>
          topicPartitionListOffsetsResultInfoMap =
              generateListOffsetsResultInfo(topicPartitionList);
      ListOffsetsResult listOffsetsResultMock = Mockito.mock(ListOffsetsResult.class);
      when(adminClientMock.listOffsets(any())).thenReturn(listOffsetsResultMock);
      KafkaFuture<Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> mapKafkaFutureMock =
          Mockito.mock(KafkaFuture.class);
      when(listOffsetsResultMock.all()).thenReturn(mapKafkaFutureMock);
      ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResultMock =
          Mockito.mock(ListConsumerGroupOffsetsResult.class);
      KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> kafkaFutureMock =
          Mockito.mock(KafkaFuture.class);
      when(kafkaFutureMock.get()).thenReturn(topicPartitionOffsetAndMetadataMap);
      when(mapKafkaFutureMock.get()).thenReturn(topicPartitionListOffsetsResultInfoMap);
      when(adminClientMock.listConsumerGroupOffsets(eq(TEST_GROUP_ID)))
          .thenReturn(listConsumerGroupOffsetsResultMock);
      when(listConsumerGroupOffsetsResultMock.partitionsToOffsetAndMetadata())
          .thenReturn(kafkaFutureMock);

      long pendingMessages = underTest.getPendingMessages();
      // 100 + 100 + 100 - 10 - 10 - 10 = 270
      assertEquals(270, pendingMessages);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void getPendingMessages_exception() {
    try {
      when(adminClientMock.listConsumerGroupOffsets(eq(TEST_GROUP_ID)))
          .thenThrow(new RuntimeException());
      long pendingMessages = underTest.getPendingMessages();
      assertEquals(Admin.PendingNotAvailable, pendingMessages);
    } catch (Exception e) {
      fail();
    }
  }

  private Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>
      generateListOffsetsResultInfo(List<TopicPartition> topicPartitionList) {
    Function<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> valueMapper =
        (tp) ->
            new ListOffsetsResult.ListOffsetsResultInfo(
                tp.partition() + 100, Instant.now().getEpochSecond(), null);
    return topicPartitionList.stream().collect(Collectors.toMap(Function.identity(), valueMapper));
  }

  private Map<TopicPartition, OffsetAndMetadata> generateTopicPartitionOffsetMetadata(
      List<TopicPartition> topicPartitionList) {
    Function<TopicPartition, OffsetAndMetadata> valueMapper =
        (tp) -> new OffsetAndMetadata(tp.partition() + 10L);
    return topicPartitionList.stream().collect(Collectors.toMap(Function.identity(), valueMapper));
  }

  private List<TopicPartition> generateTopicPartitions() {
    return Arrays.asList(
        new TopicPartition(TEST_TOPIC, 1),
        new TopicPartition(TEST_TOPIC, 2),
        new TopicPartition(TEST_TOPIC, 3));
  }

  @Test
  public void destroy() {
    underTest.destroy();
    verify(adminClientMock, times(1)).close();
  }
}
