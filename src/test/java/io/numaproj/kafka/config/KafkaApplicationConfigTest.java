package io.numaproj.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@Slf4j
@ExtendWith(MockitoExtension.class)
public class KafkaApplicationConfigTest {
  /*

  KafkaApplicationConfig underTest;

  @BeforeEach
  public void setUp() {
      underTest = new KafkaApplicationConfig(
              Objects.requireNonNull(getClass().getClassLoader().getResource("producer.properties")).getPath(),
              Objects.requireNonNull(getClass().getClassLoader().getResource("schema.registry.properties")).getPath(),
              Objects.requireNonNull(getClass().getClassLoader().getResource("consumer.properties")).getPath()
      );
  }

  @Test
  public void sinkServer_initializeSuccess() {
      var kafkaSinker = mock(KafkaSinker.class);
      var sinkServer = underTest.sinkServer(kafkaSinker);
      assertNotNull(sinkServer);
  }

  @Test
  public void kafkaProducer_initializeSuccess() {
      try {
          var kafkaProducer = underTest.kafkaProducer();
          assertNotNull(kafkaProducer);
      } catch (Exception e) {
          fail();
      }

  }

  @Test
  public void schemaRegistryClient_initializeSuccess() {
      try {
          var schemaRegistryClient = underTest.schemaRegistryClient();
          assertNotNull(schemaRegistryClient);
      } catch (Exception e) {
          fail();
      }
  }

  @Test
  public void registry_initializeSuccess() {
      var schemaRegistryClient = mock(SchemaRegistryClient.class);
      var registry = underTest.schemaRegistry(schemaRegistryClient);
      assertNotNull(registry);
  }

   */
}
