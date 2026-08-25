package io.numaproj.kafka.config;

import java.util.List;
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
  // TODO - multiple topics support with different brokers
  // Set only when the source was configured with topicName, and always set for the sink. The sink
  // reads it directly; the source uses it to tell single-topic mode from multi-topic mode.
  private String topicName;

  // Source-only: every topic the source consumes, in configuration order and deduplicated. Holds a
  // single entry in single-topic mode, so the read path never has to special-case which field to
  // read - only isMultiTopic() decides whether global partition IDs apply.
  private List<String> topicNames;
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
   * True when the source was configured with {@code topicNames} rather than {@code topicName}.
   * Single-topic deployments stay on the pre-multi-topic read path, so they keep bare Kafka
   * partition IDs and tolerate a topic that does not exist yet.
   */
  public boolean isMultiTopic() {
    return topicName == null || topicName.isBlank();
  }
}
