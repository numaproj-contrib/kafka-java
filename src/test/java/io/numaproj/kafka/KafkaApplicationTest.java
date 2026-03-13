package io.numaproj.kafka;

import static org.junit.jupiter.api.Assertions.assertThrows;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class KafkaApplicationTest {

  @Test
  void main_missingHandler_throwsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> KafkaApplication.main(new String[] {}));
  }

  @Test
  void main_unknownHandler_throwsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> KafkaApplication.main(new String[] {"--handler=unknown"}));
  }

  @Test
  void main_consumerHandler_missingPropertiesPath_throwsException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaApplication.main(
                new String[] {"--handler=consumer", "--schemaType=raw", "--topicName=test"}));
  }

  @Test
  void main_producerHandler_missingPropertiesPath_throwsException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaApplication.main(
                new String[] {"--handler=producer", "--schemaType=raw", "--topicName=test"}));
  }
}
