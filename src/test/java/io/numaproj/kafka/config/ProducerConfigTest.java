package io.numaproj.kafka.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.numaproj.kafka.schema.ConfluentRegistry;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@Slf4j
@ExtendWith(MockitoExtension.class)
public class ProducerConfigTest {

  private static ProducerConfig configFor(String resource) {
    return new ProducerConfig(
        Objects.requireNonNull(ProducerConfigTest.class.getClassLoader().getResource(resource))
            .getPath());
  }

  private ProducerConfig underTest() {
    return configFor("producer.properties");
  }

  @Test
  public void kafkaAvroProducer_initializeSuccess() throws Exception {
    try (var producer = underTest().kafkaAvroProducer()) {
      assertNotNull(producer);
    }
  }

  @Test
  public void kafkaByteArrayProducer_initializeSuccess() throws Exception {
    try (var producer = underTest().kafkaByteArrayProducer()) {
      assertNotNull(producer);
    }
  }

  @Test
  public void schemaRegistryClient_initializeSuccess() throws Exception {
    assertNotNull(underTest().schemaRegistryClient());
  }

  @Test
  public void schemaRegistryType_defaultsToConfluent() throws Exception {
    assertEquals("confluent", underTest().schemaRegistryType());
  }

  @Test
  public void schemaRegistry_confluentByDefault() throws Exception {
    var registry = underTest().schemaRegistry();
    assertInstanceOf(ConfluentRegistry.class, registry);
    registry.close();
  }

  @Test
  public void glueRegistryType_avroProducerInitializeSuccess() throws Exception {
    // Constructing the Glue serializer makes no AWS call.
    ProducerConfig glueConfig = configFor("producer.properties.glue");
    assertEquals("glue", glueConfig.schemaRegistryType());
    try (var producer = glueConfig.kafkaAvroProducer()) {
      assertNotNull(producer);
    }
  }
}
