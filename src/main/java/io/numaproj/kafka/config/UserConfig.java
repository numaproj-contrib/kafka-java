package io.numaproj.kafka.config;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
@NoArgsConstructor
public class UserConfig {
  // TODO - multiple topics support with different brokers
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
  private OnError onError = OnError.FAIL;
}
