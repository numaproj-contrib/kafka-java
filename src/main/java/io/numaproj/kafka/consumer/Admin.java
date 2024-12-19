package io.numaproj.kafka.consumer;

import io.numaproj.kafka.config.UserConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class Admin implements DisposableBean {
  private final UserConfig userConfig;
  private final AdminClient adminClient;

  public long getPendingMessages() {
    try {
      // TODO - get group id from userConfig
      ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult =
          adminClient.listConsumerGroupOffsets("group1");
      Map<TopicPartition, OffsetSpec> topicPartitionOffsetSpecMap = new HashMap<>();
      Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap =
          listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();
      topicPartitionOffsetAndMetadataMap.forEach(
          (k, v) -> {
            if (userConfig.getTopicName().equals(k.topic())) {
              topicPartitionOffsetSpecMap.put(k, OffsetSpec.latest());
            }
          });
      // TODO - change to debug
      log.info("Topic Partition Offset MetaData Map: {}", topicPartitionOffsetAndMetadataMap);
      // Get latest Offset
      ListOffsetsResult listOffsetsResult = adminClient.listOffsets(topicPartitionOffsetSpecMap);
      Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>
          topicPartitionListOffsetsResultInfoMap = listOffsetsResult.all().get();
      long totalPending =
          topicPartitionListOffsetsResultInfoMap.keySet().stream()
              .filter(k -> userConfig.getTopicName().equals(k.topic()))
              .map(
                  k -> {
                    OffsetAndMetadata currentOffset = topicPartitionOffsetAndMetadataMap.get(k);
                    ListOffsetsResult.ListOffsetsResultInfo latestOffset =
                        topicPartitionListOffsetsResultInfoMap.get(k);
                    // TODO - change to debug
                    log.info(
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
      log.info("Total Pending Messages: {}", totalPending);
      return totalPending;
    } catch (ExecutionException | InterruptedException e) {
      log.error("Failed to get pending messages", e);
      // TODO - we have a const PendingNotAvailable = int64(math.MinInt64)
      return -1;
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
