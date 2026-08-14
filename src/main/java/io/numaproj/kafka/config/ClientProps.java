package io.numaproj.kafka.config;

import io.numaproj.kafka.common.EnvVarInterpolator;
import io.numaproj.kafka.common.aws.AwsCredentials;
import io.numaproj.kafka.encryption.EncryptionProps;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * The properties-file plumbing shared by {@link ProducerConfig} and {@link ConsumerConfig}: loads
 * the file once (interpolating env vars and merging {@code KAFKA_CREDENTIAL_PROPERTIES}), hands out
 * copies because callers mutate what they get, and strips the kafka-java-managed keys before the
 * properties reach a Kafka client.
 */
final class ClientProps {

  private final String propertiesFilePath;

  // Loaded once; load() hands out copies because callers mutate what they get.
  private Properties cached;

  ClientProps(String propertiesFilePath) {
    this.propertiesFilePath = propertiesFilePath;
  }

  synchronized Properties load() throws IOException {
    if (cached == null) {
      Properties props = new Properties();
      try (InputStream is = new FileInputStream(this.propertiesFilePath)) {
        props.load(is);
      }
      EnvVarInterpolator.interpolate(props);
      loadCredentialProperties(props);
      cached = props;
    }
    Properties copy = new Properties();
    copy.putAll(cached);
    return copy;
  }

  /** Merge credential properties supplied via the KAFKA_CREDENTIAL_PROPERTIES env var. */
  private static void loadCredentialProperties(Properties props) throws IOException {
    String credentialProperties = System.getenv("KAFKA_CREDENTIAL_PROPERTIES");
    if (credentialProperties != null && !credentialProperties.isEmpty()) {
      try (StringReader sr = new StringReader(credentialProperties)) {
        props.load(sr);
      }
      EnvVarInterpolator.interpolate(props);
    }
  }

  /**
   * Remove the keys every client path manages internally, so they are not passed to Kafka clients
   * as unknown configs: {@code schema.registry.type}, the {@code payload.envelope.encryption.*}
   * family, and {@code assumeRoleArn}.
   */
  static void stripManagedProps(Properties props) {
    props.remove(SerializationProps.SCHEMA_REGISTRY_TYPE);
    props.keySet().removeIf(k -> k instanceof String s && s.startsWith(EncryptionProps.PREFIX));
    props.remove(AwsCredentials.ASSUME_ROLE_ARN);
  }

  /** The properties as the config map a {@code Serializer}/{@code Deserializer} is configured with. */
  static Map<String, Object> toConfigMap(Properties props) {
    Map<String, Object> configs = new HashMap<>();
    for (String name : props.stringPropertyNames()) {
      configs.put(name, props.getProperty(name));
    }
    return configs;
  }
}
