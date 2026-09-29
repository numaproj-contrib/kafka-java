package io.numaproj.kafka.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class EnvVarInterpolatorTest {

  @Test
  public void interpolate_replacesKnownEnvVarPlaceholders() {
    Properties props = new Properties();
    props.setProperty("group.instance.id", "my-instance-${NUMAFLOW_REPLICA}");
    props.setProperty("bootstrap.servers", "${BOOTSTRAP}");

    EnvVarInterpolator.interpolate(
        props, Map.of("NUMAFLOW_REPLICA", "2", "BOOTSTRAP", "broker:9092"));

    assertEquals("my-instance-2", props.getProperty("group.instance.id"));
    assertEquals("broker:9092", props.getProperty("bootstrap.servers"));
  }

  @Test
  public void interpolate_missingEnvVar_throwsAtStartup() {
    Properties props = new Properties();
    props.setProperty("group.instance.id", "my-instance-${MISSING}");

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> EnvVarInterpolator.interpolate(props, Map.of("NUMAFLOW_REPLICA", "2")));
    assertTrue(e.getMessage().contains("MISSING"));
    assertTrue(e.getMessage().contains("group.instance.id"));
  }

  @Test
  public void interpolate_multipleMissingVars_namesAllInException() {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "${KAFKA_BOOTSTRAP}");
    props.setProperty("sasl.jaas.config", "password=${KAFKA_PASSWORD}");

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> EnvVarInterpolator.interpolate(props, Map.of()));
    assertTrue(e.getMessage().contains("KAFKA_BOOTSTRAP"));
    assertTrue(e.getMessage().contains("KAFKA_PASSWORD"));
  }

  @Test
  public void interpolate_partialResolution_throwsForUnsetVar() {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "${KAFKA_BOOTSTRAP}");
    props.setProperty("group.id", "${KAFKA_GROUP}");

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> EnvVarInterpolator.interpolate(props, Map.of("KAFKA_GROUP", "my-group")));
    assertTrue(e.getMessage().contains("KAFKA_BOOTSTRAP"));
    assertTrue(e.getMessage().contains("bootstrap.servers"));
  }

  @Test
  public void interpolate_supportsMultiplePlaceholdersInSingleValue() {
    Properties props = new Properties();
    props.setProperty("x", "${A}-${B}-${A}");

    EnvVarInterpolator.interpolate(props, Map.of("A", "foo", "B", "bar"));

    assertEquals("foo-bar-foo", props.getProperty("x"));
  }
}

