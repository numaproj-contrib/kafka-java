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
    private String topicName;
}
