package io.numaproj.kafka.config;

import lombok.*;
import lombok.extern.slf4j.Slf4j;

@Getter
@Setter
@ToString
@EqualsAndHashCode
@Builder
@Slf4j
@AllArgsConstructor
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
}
