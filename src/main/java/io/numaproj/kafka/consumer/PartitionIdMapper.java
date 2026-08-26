package io.numaproj.kafka.consumer;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.kafka.common.TopicPartition;

/**
 * Assigns each Kafka {@code (topic, partition)} pair the distinct partition ID Numaflow watermarks
 * it by. Kafka numbers partitions from 0 within a topic, so under multi-topic two topics would
 * otherwise both claim ID 0; Numaflow publishes one watermark per ID and ratchets it forward, so a
 * shared ID takes the newer of the two event times and the slower topic's records arrive late.
 *
 * <p>Topics are sorted by name and each is given a contiguous range of {@code slotSize} IDs, where
 * {@code slotSize} is the largest partition count across all configured topics: {@code
 * globalPartitionId = sortedIndex * slotSize + kafkaPartition}. Sorting the <em>configured</em>
 * topics rather than the assigned ones makes the mapping a pure function of the configuration, so
 * every replica in the consumer group computes the same IDs with no coordination, whatever Kafka
 * assigned it. A single topic sorts to index 0 and so keeps its bare Kafka partition numbers, which
 * is what lets an existing deployment upgrade without orphaning its watermark state.
 *
 * <p>This mirrors {@code compute_topic_partition_offsets} in Numaflow's builtin Rust Kafka source.
 * Unlike that implementation, every problem here is rejected up front rather than defaulted around,
 * because each of its fallbacks silently aliases one topic's watermark onto another's.
 *
 * <p>Instances are immutable and safe to share across threads.
 */
public final class PartitionIdMapper {

  /** Numaflow narrows the partition ID to a u16, so a larger ID would alias onto another. */
  static final int MAX_GLOBAL_PARTITION_ID = 65535;

  private final Map<String, Integer> baseByTopic;
  private final int slotSize;

  private PartitionIdMapper(Map<String, Integer> baseByTopic, int slotSize) {
    this.baseByTopic = baseByTopic;
    this.slotSize = slotSize;
  }

  /**
   * Builds a mapper over the configured topics and their current partition counts.
   *
   * @param partitionCountsByTopic partition count per topic, as reported by the broker
   * @throws IllegalArgumentException if no topics are given, if any topic is absent or reports no
   *     partitions, or if the topics need more IDs than Numaflow can represent
   */
  public static PartitionIdMapper of(Map<String, Integer> partitionCountsByTopic) {
    if (partitionCountsByTopic == null || partitionCountsByTopic.isEmpty()) {
      throw new IllegalArgumentException("At least one topic is required to map partition IDs");
    }
    partitionCountsByTopic.forEach(
        (topic, partitionCount) -> {
          if (partitionCount == null || partitionCount <= 0) {
            throw new IllegalArgumentException(
                "Topic "
                    + topic
                    + " reports "
                    + partitionCount
                    + " partitions; it must exist and have at least one");
          }
        });

    int slotSize = Collections.max(partitionCountsByTopic.values());
    long highestId = (long) partitionCountsByTopic.size() * slotSize - 1;
    if (highestId > MAX_GLOBAL_PARTITION_ID) {
      throw new IllegalArgumentException(
          "%d topics of up to %d partitions each need partition IDs up to %d, more than the %d Numaflow can represent"
              .formatted(
                  partitionCountsByTopic.size(), slotSize, highestId, MAX_GLOBAL_PARTITION_ID));
    }

    // TreeMap sorts the topics, so the bases below do not depend on the caller's iteration order.
    Map<String, Integer> baseByTopic = new LinkedHashMap<>();
    int sortedIndex = 0;
    for (String topic : new TreeMap<>(partitionCountsByTopic).keySet()) {
      baseByTopic.put(topic, sortedIndex++ * slotSize);
    }
    return new PartitionIdMapper(Collections.unmodifiableMap(baseByTopic), slotSize);
  }

  /**
   * Maps a Kafka partition to its Numaflow partition ID.
   *
   * @throws IllegalArgumentException if the topic was not configured, or if the partition is beyond
   *     the topic's reserved range because partitions were added since startup
   */
  public int globalPartitionId(String topic, int kafkaPartition) {
    Integer base = baseByTopic.get(topic);
    if (base == null) {
      throw new IllegalArgumentException(
          "Topic " + topic + " is not configured; configured topics are " + baseByTopic.keySet());
    }
    if (kafkaPartition < 0 || kafkaPartition >= slotSize) {
      // Restarting recomputes the map against the current partition counts, so an operator who
      // scaled a topic past the slot size gets a correct map on the next pod.
      throw new IllegalArgumentException(
          "Partition %d of topic %s is outside the %d reserved for it; restart to remap partition IDs against the current partition counts"
              .formatted(kafkaPartition, topic, slotSize));
    }
    return base + kafkaPartition;
  }

  /** Maps a Kafka partition to its Numaflow partition ID. */
  public int globalPartitionId(TopicPartition topicPartition) {
    return globalPartitionId(topicPartition.topic(), topicPartition.partition());
  }

  /** The configured topics. */
  public Set<String> topics() {
    return baseByTopic.keySet();
  }

  /** The number of partition IDs reserved for each topic. */
  public int slotSize() {
    return slotSize;
  }

  /**
   * Renders the mapping as a startup diagnostic, e.g. {@code {orders=0, payments=8} slotSize=8}.
   */
  @Override
  public String toString() {
    return baseByTopic + " slotSize=" + slotSize;
  }
}
