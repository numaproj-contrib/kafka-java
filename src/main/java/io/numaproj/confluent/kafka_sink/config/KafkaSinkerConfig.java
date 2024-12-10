package io.numaproj.confluent.kafka_sink.config;

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
public class KafkaSinkerConfig {
    private String topicName;
}
