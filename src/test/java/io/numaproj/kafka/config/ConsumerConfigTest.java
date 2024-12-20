package io.numaproj.kafka.config;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@Slf4j
@ExtendWith(MockitoExtension.class)
public class ConsumerConfigTest {

  ConsumerConfig underTest;

  @BeforeEach
  public void setUp() {
    underTest =
        new ConsumerConfig(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("consumer/consumer.properties"))
                .getPath());
  }

  @Test
  public void consumer_initializeSuccess() {
    try {
      var kafkaConsumer = underTest.kafkaConsumer();
      assertNotNull(kafkaConsumer);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void consumer_groupIdNotSpecified() {
    underTest =
        new ConsumerConfig(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("consumer/consumer.properties.no.group.id"))
                .getPath());
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          underTest.kafkaConsumer();
        });
  }

  @Test
  public void consumer_overrideAutoCommitEnableToFalse() {
    // FIXME - figure out a way to verify the override
    underTest =
        new ConsumerConfig(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("consumer/consumer.properties.auto.commit.enabled"))
                .getPath());
    try {
      var kafkaConsumer = underTest.kafkaConsumer();
      assertNotNull(kafkaConsumer);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void adminClient_initializeSuccess() {
    try {
      var adminClient = underTest.kafkaAdminClient();
      assertNotNull(adminClient);
    } catch (Exception e) {
      fail();
    }
  }
}
