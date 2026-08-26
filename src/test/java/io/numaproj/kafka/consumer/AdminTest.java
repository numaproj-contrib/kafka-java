package io.numaproj.kafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.*;

import io.numaproj.kafka.config.UserConfig;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
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
  private static final String OTHER_TOPIC = "other-topic";
  private static final String TEST_GROUP_ID = "test-group-id";

  private Admin underTest;

  @BeforeEach
  public void setUp() {
    // Stubbed before the Admin is built: it reads the topics once, in its constructor.
    when(userConfigMock.getTopicNames()).thenReturn(List.of(TEST_TOPIC));
    underTest = new Admin(userConfigMock, TEST_GROUP_ID, adminClientMock);
  }

  @Test
  public void getPendingMessages_success() {
    try {
      stubConsumerGroupOffsets(generateTopicPartitions());

      long pendingMessages = underTest.getPendingMessages();
      // 100 + 100 + 100 - 10 - 10 - 10 = 270
      assertEquals(270, pendingMessages);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void getPendingMessages_multipleTopics_sumsTheLagAcrossAllOfThem() {
    try {
      when(userConfigMock.getTopicNames()).thenReturn(List.of(TEST_TOPIC, OTHER_TOPIC));
      underTest = new Admin(userConfigMock, TEST_GROUP_ID, adminClientMock);
      stubConsumerGroupOffsets(
          List.of(new TopicPartition(TEST_TOPIC, 1), new TopicPartition(OTHER_TOPIC, 2)));

      // (101 - 11) + (102 - 12) = 180
      assertEquals(180, underTest.getPendingMessages());
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void getPendingMessages_topicInTheGroupButNotConfigured_isExcluded() {
    try {
      // A consumer group outlives a config change, so it can still hold offsets for a topic this
      // source no longer reads; counting those would overstate the backlog and over-scale.
      stubConsumerGroupOffsets(
          List.of(new TopicPartition(TEST_TOPIC, 1), new TopicPartition(OTHER_TOPIC, 2)));

      // Only test-topic partition 1 counts: 101 - 11 = 90.
      assertEquals(90, underTest.getPendingMessages());
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

  /**
   * Stubs the group as holding a committed offset on each of {@code topicPartitionList}.
   * listOffsets answers only for the partitions it was actually asked about, so the topic filter
   * under test genuinely decides what gets summed.
   */
  private void stubConsumerGroupOffsets(List<TopicPartition> topicPartitionList) throws Exception {
    Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap =
        generateTopicPartitionOffsetMetadata(topicPartitionList);
    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
        generateListOffsetsResultInfo(topicPartitionList);

    ListOffsetsResult listOffsetsResultMock = Mockito.mock(ListOffsetsResult.class);
    when(adminClientMock.listOffsets(any()))
        .thenAnswer(
            invocation -> {
              Map<TopicPartition, ?> requested = invocation.getArgument(0);
              Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> answered =
                  new HashMap<>(latestOffsets);
              answered.keySet().retainAll(requested.keySet());
              when(listOffsetsResultMock.all()).thenReturn(KafkaFuture.completedFuture(answered));
              return listOffsetsResultMock;
            });

    ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResultMock =
        Mockito.mock(ListConsumerGroupOffsetsResult.class);
    when(adminClientMock.listConsumerGroupOffsets(eq(TEST_GROUP_ID)))
        .thenReturn(listConsumerGroupOffsetsResultMock);
    when(listConsumerGroupOffsetsResultMock.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(topicPartitionOffsetAndMetadataMap));
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
  public void close() {
    underTest.close();
    verify(adminClientMock, times(1)).close();
  }

  @Test
  public void partitionCounts_returnsThePartitionCountOfEachTopic() throws Exception {
    stubDescribeTopics(
        KafkaFuture.completedFuture(
            Map.of(
                "orders", topicDescription("orders", 8),
                "payments", topicDescription("payments", 4))));

    assertEquals(
        Map.of("orders", 8, "payments", 4),
        underTest.partitionCounts(List.of("orders", "payments")));
  }

  @Test
  public void partitionCounts_whenATopicDoesNotExist_thenPropagatesTheFailure() {
    // The source must not start on an understated partition count: the topic appearing later
    // would overlap another topic's range of partition IDs.
    KafkaFuture<Map<String, TopicDescription>> failed = mock(KafkaFuture.class);
    try {
      when(failed.get())
          .thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("orders")));
    } catch (Exception e) {
      fail();
    }
    stubDescribeTopics(failed);

    assertThrows(ExecutionException.class, () -> underTest.partitionCounts(List.of("orders")));
  }

  private void stubDescribeTopics(KafkaFuture<Map<String, TopicDescription>> allTopicNames) {
    DescribeTopicsResult describeTopicsResultMock = mock(DescribeTopicsResult.class);
    when(adminClientMock.describeTopics(anyCollection())).thenReturn(describeTopicsResultMock);
    when(describeTopicsResultMock.allTopicNames()).thenReturn(allTopicNames);
  }

  private static TopicDescription topicDescription(String topic, int partitionCount) {
    List<TopicPartitionInfo> partitions = new ArrayList<>();
    for (int partition = 0; partition < partitionCount; partition++) {
      partitions.add(new TopicPartitionInfo(partition, null, List.of(), List.of()));
    }
    return new TopicDescription(topic, false, partitions);
  }
}
