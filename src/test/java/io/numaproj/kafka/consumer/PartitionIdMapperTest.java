package io.numaproj.kafka.consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class PartitionIdMapperTest {

  @Test
  void toGlobalId_singleTopic_equalsRawPartition() {
    PartitionIdMapper mapper = new PartitionIdMapper(List.of("topic-a"));
    assertEquals(0, mapper.toGlobalId("topic-a", 0));
    assertEquals(5, mapper.toGlobalId("topic-a", 5));
  }

  @Test
  void toGlobalId_multipleTopics_usesAlphabeticalIndexAndStride() {
    PartitionIdMapper mapper = new PartitionIdMapper(List.of("topic-a", "topic-b", "topic-c"));
    // topic-a → block 0 (0..255), topic-b → block 1 (256..511), topic-c → block 2 (512..767)
    assertEquals(0, mapper.toGlobalId("topic-a", 0));
    assertEquals(255, mapper.toGlobalId("topic-a", 255));
    assertEquals(256, mapper.toGlobalId("topic-b", 0));
    assertEquals(511, mapper.toGlobalId("topic-b", 255));
    assertEquals(512, mapper.toGlobalId("topic-c", 0));
  }

  @Test
  void toGlobalId_isIndependentOfConfigOrder() {
    // The same topics in a different config order produce identical IDs (sorted alphabetically).
    PartitionIdMapper a = new PartitionIdMapper(List.of("topic-a", "topic-b"));
    PartitionIdMapper b = new PartitionIdMapper(List.of("topic-b", "topic-a"));
    assertEquals(a.toGlobalId("topic-b", 3), b.toGlobalId("topic-b", 3));
    assertEquals(256 + 3, a.toGlobalId("topic-b", 3));
  }

  @Test
  void toGlobalId_unknownTopic_throws() {
    PartitionIdMapper mapper = new PartitionIdMapper(List.of("topic-a"));
    assertThrows(IllegalArgumentException.class, () -> mapper.toGlobalId("topic-x", 0));
  }

  @Test
  void constructor_atMostMaxTopics_isAllowed() {
    List<String> topics =
        IntStream.range(0, PartitionIdMapper.MAX_TOPICS)
            .mapToObj(i -> "topic-" + i)
            .collect(Collectors.toList());
    assertDoesNotThrow(() -> new PartitionIdMapper(topics));
  }

  @Test
  void constructor_tooManyTopics_throws() {
    List<String> topics =
        IntStream.range(0, PartitionIdMapper.MAX_TOPICS + 1)
            .mapToObj(i -> "topic-" + i)
            .collect(Collectors.toList());
    assertThrows(IllegalArgumentException.class, () -> new PartitionIdMapper(topics));
  }

  @Test
  void validatePartitionCounts_withinCap_doesNotThrow() {
    PartitionIdMapper mapper = new PartitionIdMapper(List.of("topic-a", "topic-b"));
    Map<String, Integer> counts =
        Map.of("topic-a", PartitionIdMapper.MAX_PARTITIONS_PER_TOPIC, "topic-b", 1);
    assertDoesNotThrow(() -> mapper.validatePartitionCounts(counts));
  }

  @Test
  void validatePartitionCounts_exceedsCap_throws() {
    PartitionIdMapper mapper = new PartitionIdMapper(List.of("topic-a"));
    Map<String, Integer> counts =
        Map.of("topic-a", PartitionIdMapper.MAX_PARTITIONS_PER_TOPIC + 1);
    assertThrows(IllegalArgumentException.class, () -> mapper.validatePartitionCounts(counts));
  }
}
