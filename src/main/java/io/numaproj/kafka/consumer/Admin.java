package io.numaproj.kafka.consumer;

import io.numaproj.kafka.config.UserConfig;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class Admin {

  // PendingNotAvailable is used to indicate that the pending count is not available
  // It matches PendingNotAvailable defined in
  // https://github.com/numaproj/numaflow/blob/main/pkg/isb/interfaces.go#L31
  static final long PendingNotAvailable = Long.MIN_VALUE;

  private final UserConfig userConfig;
  private final String consumerGroupId;
  private final AdminClient adminClient;

  public Admin(UserConfig userConfig, String consumerGroupId, AdminClient adminClient) {
    this.userConfig = userConfig;
    this.consumerGroupId = consumerGroupId;
    this.adminClient = adminClient;
  }

  public long getPendingMessages() {
    try {
      ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult =
          adminClient.listConsumerGroupOffsets(consumerGroupId);
      log.debug("listConsumerGroupOffsetsResult result: {}", listConsumerGroupOffsetsResult);
      Map<TopicPartition, OffsetSpec> topicPartitionOffsetSpecMap = new HashMap<>();
      Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap =
          listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();
      topicPartitionOffsetAndMetadataMap.forEach(
          (k, v) -> {
            if (userConfig.getTopicName().equals(k.topic())) {
              topicPartitionOffsetSpecMap.put(k, OffsetSpec.latest());
            }
          });
      log.debug("Topic Partition Offset Metadata Map: {}", topicPartitionOffsetAndMetadataMap);
      // Get the latest Offsets for the topic partitions
      ListOffsetsResult listOffsetsResult = adminClient.listOffsets(topicPartitionOffsetSpecMap);
      Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>
          topicPartitionListOffsetsResultInfoMap = listOffsetsResult.all().get();
      long totalPending =
          topicPartitionListOffsetsResultInfoMap.keySet().stream()
              .map(
                  k -> {
                    OffsetAndMetadata currentOffset = topicPartitionOffsetAndMetadataMap.get(k);
                    ListOffsetsResult.ListOffsetsResultInfo latestOffset =
                        topicPartitionListOffsetsResultInfoMap.get(k);
                    log.debug(
                        "topic:{}, partition:{}, current offset:{}, latest offset:{}, pending count:{}",
                        k.topic(),
                        k.partition(),
                        currentOffset.offset(),
                        latestOffset.offset(),
                        latestOffset.offset() - currentOffset.offset());
                    return latestOffset.offset() - currentOffset.offset();
                  })
              .mapToLong(Long::longValue)
              .sum();
      log.debug("Total Pending Messages: {}", totalPending);
      return totalPending;
    } catch (Exception e) {
      log.error("Failed to get pending messages", e);
      return PendingNotAvailable;
    }
  }

  /**
   * Looks up how many partitions each topic currently has, so the partition ID map can be built
   * before the first poll.
   *
   * @throws Exception if the metadata call fails, notably {@code UnknownTopicOrPartitionException}
   *     if a topic does not exist - the source must not start with an understated partition count,
   *     because the topic appearing later would overlap another topic's ID range
   */
  public Map<String, Integer> partitionCounts(Collection<String> topics) throws Exception {
    Map<String, TopicDescription> descriptions =
        adminClient.describeTopics(topics).allTopicNames().get();
    Map<String, Integer> partitionCounts = new HashMap<>();
    descriptions.forEach(
        (topic, description) -> partitionCounts.put(topic, description.partitions().size()));
    log.info("Partition counts: {}", partitionCounts);
    return partitionCounts;
  }

  public void close() {
    log.info("Shutting down the Kafka admin client");
    if (adminClient != null) {
      adminClient.close();
    }
    log.info("Kafka admin client shutdown complete");
  }

}
