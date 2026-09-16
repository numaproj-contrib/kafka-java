package io.numaproj.kafka.config;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class UserConfig {
  // The configured topic(s): a single name, or a comma-separated list of topics on the same cluster
  // (see getTopics()).
  private String topicName;
  // TODO - enum for different schema types
  // TODO - technically this field can be derived from schema registry
  //  Figure out a way to do that and remove this field.
  private String schemaType;

  // optional schema subject and version if user wants to use a specific schema
  private String schemaSubject;
  private int schemaVersion;

  // Source-only: how the source reacts to a record that fails to be read. UserConfig is shared with
  // the sink, so a producer deployment setting this key is silently ignored.
  @Builder.Default private OnError onError = OnError.FAIL;

  /**
   * Splits {@link #topicName} into the list of topics to consume from. The value is split on commas
   * and each name is trimmed; empty entries are dropped. A single name with no commas yields a
   * one-element list.
   *
   * @return the configured topics, or an empty list if {@code topicName} is null or blank
   */
  public List<String> getTopics() {
    if (topicName == null || topicName.isBlank()) {
      return List.of();
    }
    return Arrays.stream(topicName.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }
}
