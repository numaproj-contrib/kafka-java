package io.numaproj.kafka.config;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

@Slf4j
@ExtendWith(MockitoExtension.class)
public class ProducerConfigTest {

  ProducerConfig underTest;

  @BeforeEach
  public void setUp() {
    underTest =
        new ProducerConfig(
            Objects.requireNonNull(getClass().getClassLoader().getResource("producer.properties"))
                .getPath(),
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("schema.registry.properties"))
                .getPath());
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
}
