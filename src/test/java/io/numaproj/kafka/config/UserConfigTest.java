package io.numaproj.kafka.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class UserConfigTest {

  @Test
  void getTopics_singleName_returnsOneTopic() {
    assertEquals(List.of("topic-a"), UserConfig.builder().topicName("topic-a").build().getTopics());
  }

  @Test
  void getTopics_commaSeparated_splitsAndTrimsEachName() {
    assertEquals(
        List.of("topic-a", "topic-b", "topic-c"),
        UserConfig.builder().topicName("topic-a, topic-b ,  topic-c").build().getTopics());
  }

  @Test
  void getTopics_trailingAndEmptyEntries_areDropped() {
    assertEquals(
        List.of("topic-a", "topic-b"),
        UserConfig.builder().topicName("topic-a,,topic-b,").build().getTopics());
  }

  @Test
  void getTopics_nullTopicName_returnsEmptyList() {
    assertEquals(List.of(), UserConfig.builder().build().getTopics());
  }

  @Test
  void getTopics_blankTopicName_returnsEmptyList() {
    assertEquals(List.of(), UserConfig.builder().topicName("  ").build().getTopics());
  }

  @Test
  void getTopics_onlyCommas_returnsEmptyList() {
    assertEquals(List.of(), UserConfig.builder().topicName(" , , ").build().getTopics());
  }
}
