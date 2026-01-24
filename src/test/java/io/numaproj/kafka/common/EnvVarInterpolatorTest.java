package io.numaproj.kafka.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
  public void interpolate_leavesUnknownEnvVarPlaceholdersUnchanged() {
    Properties props = new Properties();
    props.setProperty("group.instance.id", "my-instance-${MISSING}");

    EnvVarInterpolator.interpolate(props, Map.of("NUMAFLOW_REPLICA", "2"));

    assertEquals("my-instance-${MISSING}", props.getProperty("group.instance.id"));
  }

  @Test
  public void interpolate_supportsMultiplePlaceholdersInSingleValue() {
    Properties props = new Properties();
    props.setProperty("x", "${A}-${B}-${A}");

    EnvVarInterpolator.interpolate(props, Map.of("A", "foo", "B", "bar"));

    assertEquals("foo-bar-foo", props.getProperty("x"));
  }
}

