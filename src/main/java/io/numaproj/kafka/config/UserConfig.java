package io.numaproj.kafka.config;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@ToString
@EqualsAndHashCode
@Builder
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
@Configuration
@org.springframework.boot.context.properties.EnableConfigurationProperties
@ConfigurationProperties(ignoreInvalidFields = true)
public class UserConfig {
  // TODO - multiple topics support with different brokers
  private String topicName;
  // FIXME - this is duplicate of the group.id in the consumer properties
  //  Figure out a way to maintain single source of truth.
  //  User shouldn't need to declare group id twice.
  private String groupId;
  // TODO - enum for different schema types
  // TODO - technically this field can be derived from schema registry
  //  Figure out a way to do that and remove this field.
  private String schemaType;

  // optional schema subject and version if user wants to use a specific schema
  private String schemaSubject;
  private int schemaVersion;
}
