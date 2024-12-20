package io.numaproj.kafka.consumer;

import io.numaproj.kafka.config.UserConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class Admin implements DisposableBean {

  // PendingNotAvailable is used to indicate that the pending count is not available
  // It matches PendingNotAvailable defined in
  // https://github.com/numaproj/numaflow/blob/main/pkg/isb/interfaces.go#L31
  private static final long PendingNotAvailable = Long.MIN_VALUE;

  private final UserConfig userConfig;
  private final AdminClient adminClient;

  @Autowired
  public Admin(UserConfig userConfig, AdminClient adminClient) {
    this.userConfig = userConfig;
    this.adminClient = adminClient;
  }

  public long getPendingMessages() {
    try {
      ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult =
          adminClient.listConsumerGroupOffsets(userConfig.getGroupId());
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
      // Get latest Offset
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
    } catch (ExecutionException | InterruptedException e) {
      log.error("Failed to get pending messages", e);
      return PendingNotAvailable;
    }
  }

  @Override
  public void destroy() {
    log.info("Shutting down the admin client");
    if (adminClient != null) {
      adminClient.close();
    }
  }
}
