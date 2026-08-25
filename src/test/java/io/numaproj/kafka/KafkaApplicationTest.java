package io.numaproj.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.numaproj.kafka.config.OnError;
import io.numaproj.kafka.config.UserConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

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

  @Test
  void buildUserConfig_topicNameOnly_thenTopicNamesHoldsThatOneTopic() {
    UserConfig userConfig = KafkaApplication.buildUserConfig(consumerArgs());

    assertEquals("test", userConfig.getTopicName());
    assertEquals(List.of("test"), userConfig.getTopicNames());
    assertFalse(userConfig.isMultiTopic());
  }

  @Test
  void buildUserConfig_topicNamesCommaSeparated_thenParsedInOrder() {
    UserConfig userConfig = KafkaApplication.buildUserConfig(multiTopicArgs("orders,payments"));

    assertEquals(List.of("orders", "payments"), userConfig.getTopicNames());
    assertTrue(userConfig.isMultiTopic());
    assertNull(userConfig.getTopicName());
  }

  @Test
  void buildUserConfig_topicNamesWithSurroundingWhitespace_thenEntriesAreTrimmed() {
    UserConfig userConfig = KafkaApplication.buildUserConfig(multiTopicArgs(" orders , payments "));

    assertEquals(List.of("orders", "payments"), userConfig.getTopicNames());
  }

  @Test
  void buildUserConfig_bothTopicNameAndTopicNames_throwsAtStartup() {
    Map<String, String> argMap = consumerArgs();
    argMap.put("topicNames", "orders,payments");

    assertThrows(IllegalArgumentException.class, () -> KafkaApplication.buildUserConfig(argMap));
  }

  @Test
  void buildUserConfig_neitherTopicNameNorTopicNames_throwsAtStartup() {
    Map<String, String> argMap = new HashMap<>();
    argMap.put("schemaType", "raw");

    assertThrows(IllegalArgumentException.class, () -> KafkaApplication.buildUserConfig(argMap));
  }

  @Test
  void buildUserConfig_duplicateTopicNames_thenDedupedAndWarned() {
    List<ILoggingEvent> warnings =
        captureWarnings(
            () ->
                assertEquals(
                    List.of("orders", "payments"),
                    KafkaApplication.buildUserConfig(multiTopicArgs("orders,payments,orders"))
                        .getTopicNames()));

    assertEquals(1, warnings.size());
    assertTrue(warnings.get(0).getFormattedMessage().contains("orders"));
  }

  @Test
  void buildUserConfig_blankTopicNamesEntry_throwsAtStartup() {
    assertThrows(
        IllegalArgumentException.class,
        () -> KafkaApplication.buildUserConfig(multiTopicArgs("orders,,payments")));
  }

  @Test
  void buildUserConfig_trailingCommaInTopicNames_throwsAtStartup() {
    assertThrows(
        IllegalArgumentException.class,
        () -> KafkaApplication.buildUserConfig(multiTopicArgs("orders,")));
  }

  @Test
  void main_producerHandlerWithTopicNames_throwsException() {
    // The properties path is supplied so the failure can only be the multi-topic rejection.
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                KafkaApplication.main(
                    new String[] {
                      "--handler=producer",
                      "--schemaType=raw",
                      "--topicNames=orders,payments",
                      "--producer.properties.path=/does/not/matter"
                    }));

    assertTrue(thrown.getMessage().contains("--topicNames"));
  }

  @Test
  void main_configFileWithTopicNamesList_thenEachEntryIsATopic(@TempDir Path tempDir)
      throws Exception {
    Path configFile = tempDir.resolve("config.yaml");
    Files.writeString(
        configFile,
        """
        schemaType: raw
        topicNames:
          - orders
          - payments
        """);

    UserConfig userConfig =
        KafkaApplication.buildUserConfig(
            KafkaApplication.parseArgs(new String[] {"--config=" + configFile}));

    assertEquals(List.of("orders", "payments"), userConfig.getTopicNames());
  }

  @Test
  void main_configFileWithTopicName_thenStaysSingleTopic(@TempDir Path tempDir) throws Exception {
    Path configFile = tempDir.resolve("config.yaml");
    Files.writeString(configFile, "schemaType: raw\ntopicName: orders\n");

    UserConfig userConfig =
        KafkaApplication.buildUserConfig(
            KafkaApplication.parseArgs(new String[] {"--config=" + configFile}));

    assertEquals(List.of("orders"), userConfig.getTopicNames());
    assertFalse(userConfig.isMultiTopic());
  }

  private static Map<String, String> multiTopicArgs(String topicNames) {
    Map<String, String> argMap = new HashMap<>();
    argMap.put("topicNames", topicNames);
    argMap.put("schemaType", "raw");
    return argMap;
  }

  private static List<ILoggingEvent> captureWarnings(Runnable action) {
    Logger logger = (Logger) LoggerFactory.getLogger(KafkaApplication.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      action.run();
    } finally {
      logger.detachAppender(appender);
    }
    return appender.list.stream().filter(e -> e.getLevel() == Level.WARN).toList();
  }
}
