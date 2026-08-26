package io.numaproj.kafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class PartitionIdMapperTest {

  @Test
  void globalPartitionId_singleTopic_thenKeepsTheKafkaPartitionNumbers() {
    // The upgrade guarantee: a deployment that adds no topics keeps the partition IDs its
    // Numaflow watermark entities are already named after.
    PartitionIdMapper underTest = PartitionIdMapper.of(Map.of("orders", 8));

    for (int partition = 0; partition < 8; partition++) {
      assertEquals(partition, underTest.globalPartitionId("orders", partition));
    }
  }

  @Test
  void globalPartitionId_twoTopics_thenEachTopicGetsARangeOfTheLargestPartitionCount() {
    PartitionIdMapper underTest = PartitionIdMapper.of(Map.of("orders", 8, "payments", 4));

    assertEquals(8, underTest.slotSize());
    assertEquals(0, underTest.globalPartitionId("orders", 0));
    assertEquals(7, underTest.globalPartitionId("orders", 7));
    assertEquals(8, underTest.globalPartitionId("payments", 0));
    assertEquals(11, underTest.globalPartitionId("payments", 3));
  }

  @Test
  void globalPartitionId_topicsSortByName_thenTheSmallerTopicCanStillComeFirst() {
    // Bases follow the sorted name, not the partition count or the configuration order.
    PartitionIdMapper underTest = PartitionIdMapper.of(Map.of("alpha", 2, "beta", 6));

    assertEquals(6, underTest.slotSize());
    assertEquals(0, underTest.globalPartitionId("alpha", 0));
    assertEquals(6, underTest.globalPartitionId("beta", 0));
  }

  @Test
  void of_differentMapIterationOrders_thenProduceTheSameMapping() {
    // Every replica must agree on the IDs without coordinating, whatever order it built the map in.
    Map<String, Integer> counts = Map.of("orders", 8, "payments", 4, "shipments", 2);
    Map<String, Integer> reversed = new TreeMap<>(java.util.Comparator.reverseOrder());
    reversed.putAll(counts);
    Map<String, Integer> insertionOrdered = new LinkedHashMap<>();
    insertionOrdered.put("shipments", 2);
    insertionOrdered.put("orders", 8);
    insertionOrdered.put("payments", 4);

    PartitionIdMapper fromHashMap = PartitionIdMapper.of(new HashMap<>(counts));
    PartitionIdMapper fromReversed = PartitionIdMapper.of(reversed);
    PartitionIdMapper fromInsertionOrdered = PartitionIdMapper.of(insertionOrdered);

    for (String topic : counts.keySet()) {
      assertEquals(
          fromHashMap.globalPartitionId(topic, 0), fromReversed.globalPartitionId(topic, 0));
      assertEquals(
          fromHashMap.globalPartitionId(topic, 0), fromInsertionOrdered.globalPartitionId(topic, 0));
    }
  }

  @Test
  void globalPartitionId_topicPartition_thenMatchesTheTopicAndPartitionOverload() {
    PartitionIdMapper underTest = PartitionIdMapper.of(Map.of("orders", 8, "payments", 4));

    assertEquals(9, underTest.globalPartitionId(new TopicPartition("payments", 1)));
  }

  @Test
  void globalPartitionId_partitionWithinTheSlackOfASmallerTopic_thenStillMaps() {
    // A topic grown from 4 to 6 partitions still fits the 8 reserved for it, and no ID collides,
    // so there is no reason to fail before the operator restarts.
    PartitionIdMapper underTest = PartitionIdMapper.of(Map.of("orders", 8, "payments", 4));

    assertEquals(13, underTest.globalPartitionId("payments", 5));
  }

  @Test
  void of_emptyMap_throws() {
    assertThrows(IllegalArgumentException.class, () -> PartitionIdMapper.of(Map.of()));
  }

  @Test
  void of_nullMap_throws() {
    assertThrows(IllegalArgumentException.class, () -> PartitionIdMapper.of(null));
  }

  @Test
  void of_topicWithNoPartitions_throws() {
    // An absent topic would otherwise understate the slot size and overlap the ranges once created.
    assertThrows(
        IllegalArgumentException.class,
        () -> PartitionIdMapper.of(Map.of("orders", 8, "payments", 0)));
  }

  @Test
  void of_moreIdsThanNumaflowCanRepresent_throwsNamingTheTopicCountAndSlotSize() {
    // 65 topics of 1024 partitions reach ID 66559, past the u16 Numaflow narrows the ID to.
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> PartitionIdMapper.of(partitionCounts(65, 1024)));

    assertTrue(thrown.getMessage().contains("65"));
    assertTrue(thrown.getMessage().contains("1024"));
  }

  @Test
  void of_exactlyFillingTheIdSpace_thenTheHighestIdIsTheLargestNumaflowAccepts() {
    // 64 x 1024 is the largest budget that fits, and its last ID must land exactly on the u16 max.
    PartitionIdMapper underTest = PartitionIdMapper.of(partitionCounts(64, 1024));

    int highestId =
        underTest.topics().stream()
            .mapToInt(topic -> underTest.globalPartitionId(topic, 1023))
            .max()
            .orElseThrow();

    assertEquals(PartitionIdMapper.MAX_GLOBAL_PARTITION_ID, highestId);
  }

  @Test
  void globalPartitionId_unknownTopic_throws() {
    PartitionIdMapper underTest = PartitionIdMapper.of(Map.of("orders", 8));

    assertThrows(
        IllegalArgumentException.class, () -> underTest.globalPartitionId("payments", 0));
  }

  @Test
  void globalPartitionId_partitionBeyondTheReservedRange_throws() {
    // Partitions added past the slot size since startup would alias onto the next topic's range.
    PartitionIdMapper underTest = PartitionIdMapper.of(Map.of("orders", 8, "payments", 4));

    assertThrows(IllegalArgumentException.class, () -> underTest.globalPartitionId("orders", 8));
  }

  @Test
  void globalPartitionId_negativePartition_throws() {
    PartitionIdMapper underTest = PartitionIdMapper.of(Map.of("orders", 8));

    assertThrows(IllegalArgumentException.class, () -> underTest.globalPartitionId("orders", -1));
  }

  @Test
  void topics_returnsEveryConfiguredTopic() {
    PartitionIdMapper underTest = PartitionIdMapper.of(Map.of("orders", 8, "payments", 4));

    assertEquals(Set.of("orders", "payments"), underTest.topics());
  }

  @Test
  void toString_rendersTheBasesAndSlotSize() {
    PartitionIdMapper underTest = PartitionIdMapper.of(Map.of("orders", 8, "payments", 4));

    assertEquals("{orders=0, payments=8} slotSize=8", underTest.toString());
  }

  /** {@code topicCount} topics named {@code topic-0..n}, each with {@code partitionsPerTopic}. */
  private static Map<String, Integer> partitionCounts(int topicCount, int partitionsPerTopic) {
    Map<String, Integer> counts = new HashMap<>();
    for (int i = 0; i < topicCount; i++) {
      counts.put("topic-" + i, partitionsPerTopic);
    }
    return counts;
  }
}
