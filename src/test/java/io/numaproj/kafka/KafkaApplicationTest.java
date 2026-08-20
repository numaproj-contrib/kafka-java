package io.numaproj.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.numaproj.kafka.config.OnError;
import java.util.HashMap;
import java.util.Map;
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
        () ->
            KafkaApplication.main(
                new String[] {
                  "--handler=unknown", "--topicName=test", "--schemaType=raw"
                }));
  }

  @Test
  void main_missingTopicName_throwsException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaApplication.main(
                new String[] {"--handler=consumer", "--schemaType=raw"}));
  }

  @Test
  void main_missingSchemaType_throwsException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaApplication.main(
                new String[] {"--handler=consumer", "--topicName=test"}));
  }

  @Test
  void main_consumerHandler_missingPropertiesPath_throwsException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            KafkaApplication.main(
                new String[] {"--handler=consumer", "--schemaType=raw", "--topicName=test"}));
  }

  private static Map<String, String> consumerArgs() {
    Map<String, String> argMap = new HashMap<>();
    argMap.put("topicName", "test");
    argMap.put("schemaType", "raw");
    return argMap;
  }

  @Test
  void buildUserConfig_onErrorSkip_isParsedIntoTheConfig() {
    Map<String, String> argMap = consumerArgs();
    argMap.put("onError", "skip");

    assertEquals(OnError.SKIP, KafkaApplication.buildUserConfig(argMap).getOnError());
  }

  @Test
  void buildUserConfig_onErrorAbsent_defaultsToFail() {
    assertEquals(OnError.FAIL, KafkaApplication.buildUserConfig(consumerArgs()).getOnError());
  }

  @Test
  void buildUserConfig_onErrorInvalid_throwsAtStartup() {
    Map<String, String> argMap = consumerArgs();
    argMap.put("onError", "dead-letter");

    assertThrows(
        IllegalArgumentException.class, () -> KafkaApplication.buildUserConfig(argMap));
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
