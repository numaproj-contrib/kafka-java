package io.numaproj.kafka.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps each {@code (topic, partition)} pair to a globally unique Numaflow partition ID.
 *
 * <p>Numaflow identifies every source partition by a single integer partition ID (a {@code u16},
 * range 0–65535) used as the key for watermark tracking and idle detection. With one topic the raw
 * Kafka partition number is unique, but across multiple topics every topic numbers its partitions
 * from 0, so {@code topic-a/partition-0} and {@code topic-b/partition-0} would collide. This mapper
 * assigns each topic a reserved block of IDs using a fixed stride:
 *
 * <pre>globalPartitionId = topicIndex * STRIDE + partition</pre>
 *
 * where {@code topicIndex} is the topic's position in the configured list sorted alphabetically —
 * derived from static config only, so the mapping is deterministic across every pod and restart.
 *
 * <p>Using a fixed stride (rather than the live max partition count) keeps existing IDs stable when
 * a topic gains partitions at runtime. With a single topic {@code topicIndex} is 0, so the result
 * equals the raw partition number and behavior is unchanged.
 */
final class PartitionIdMapper {

  /**
   * Block size reserved per topic. A constant (not the live partition count) keeps IDs stable across
   * runtime partition expansion. Caps a topic at {@code STRIDE - 1} partitions and the deployment at
   * {@code 65536 / STRIDE} topics.
   */
  static final int STRIDE = 256;

  /** Max topics that fit the u16 space with the given stride: 65536 / 256 = 256. */
  static final int MAX_TOPICS = 65536 / STRIDE;

  /** Max partitions a single topic may have: STRIDE - 1 = 255. */
  static final int MAX_PARTITIONS_PER_TOPIC = STRIDE - 1;

  private final Map<String, Integer> topicIndex;

  /**
   * @param topics the configured topics; order is irrelevant because they are sorted alphabetically
   * @throws IllegalArgumentException if there are more than {@link #MAX_TOPICS} topics
   */
  PartitionIdMapper(List<String> topics) {
    if (topics.size() > MAX_TOPICS) {
      throw new IllegalArgumentException(
          "Too many topics for the partition-ID space: "
              + topics.size()
              + " > "
              + MAX_TOPICS
              + " (STRIDE="
              + STRIDE
              + "). Reduce the number of topics or raise STRIDE.");
    }
    List<String> sorted = new ArrayList<>(topics);
    Collections.sort(sorted);
    Map<String, Integer> index = new HashMap<>();
    for (int i = 0; i < sorted.size(); i++) {
      index.put(sorted.get(i), i);
    }
    this.topicIndex = Collections.unmodifiableMap(index);
  }

  /**
   * @return the globally unique partition ID for the given topic-partition
   * @throws IllegalArgumentException if the topic is not one of the configured topics
   */
  int toGlobalId(String topic, int partition) {
    Integer index = topicIndex.get(topic);
    if (index == null) {
      throw new IllegalArgumentException("Unknown topic (not in the configured list): " + topic);
    }
    return index * STRIDE + partition;
  }

  /**
   * Fails fast if any topic has more partitions than a stride block can hold, which would let its IDs
   * overflow into the next topic's block.
   *
   * @param partitionCounts partition count per topic (topics absent from the map are not checked)
   * @throws IllegalArgumentException if any topic exceeds {@link #MAX_PARTITIONS_PER_TOPIC}
   */
  void validatePartitionCounts(Map<String, Integer> partitionCounts) {
    for (Map.Entry<String, Integer> entry : partitionCounts.entrySet()) {
      if (entry.getValue() > MAX_PARTITIONS_PER_TOPIC) {
        throw new IllegalArgumentException(
            "Topic "
                + entry.getKey()
                + " has "
                + entry.getValue()
                + " partitions, exceeding the per-topic cap of "
                + MAX_PARTITIONS_PER_TOPIC
                + " (STRIDE="
                + STRIDE
                + "). Raise STRIDE to support more partitions per topic.");
      }
    }
  }
}
