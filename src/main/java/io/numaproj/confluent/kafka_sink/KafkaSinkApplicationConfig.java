package io.numaproj.confluent.kafka_sink;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.numaproj.confluent.kafka_sink.sinker.KafkaSinker;
import io.numaproj.numaflow.sinker.Server;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
@Slf4j
@ComponentScan(basePackages = "io.numaproj.confluent.kafka_sink")
public class KafkaSinkApplicationConfig {

    @Value("${producer.properties.path:NA}")
    private String producerPropertiesFilePath;

    @Value("${schema.registry.properties.path:NA}")
    private String schemaRegistryPropertiesFilePath;

    @Bean
    public Server sinkServer(KafkaSinker kafkaSinker) {
        return new Server(kafkaSinker);
    }

    @Bean
    public KafkaProducer<String, GenericRecord> kafkaProducer() throws IOException {
        log.info("Instantiating the Kafka producer from producer properties file path: {}", this.producerPropertiesFilePath);
        Properties props = new Properties();
        InputStream is = new FileInputStream(this.producerPropertiesFilePath);
        props.load(is);
        log.info("Kafka producer props read from user input ConfigMap: {}", props);
        return new KafkaProducer<>(props);
    }

    @Bean
    public SchemaRegistryClient schemaRegistryClient() throws IOException {
        log.info("Instantiating the Kafka schema registry client from producer properties file path: {}", this.producerPropertiesFilePath);
        Properties props = new Properties();
        InputStream is = new FileInputStream(this.schemaRegistryPropertiesFilePath);
        props.load(is);
        String schemaRegistryUrl = props.getProperty("schema.registry.url");
        int identityMapCapacity = Integer.parseInt(props.getProperty("schema.registry.identity.map.capacity", "100")); // Default to 100 if not specified
        String basicAuthSource = props.getProperty("basic.auth.credentials.source");
        String userInfo = props.getProperty("basic.auth.user.info");
        Map<String, String> schemaRegistryClientConfigs = new HashMap<>();
        if (basicAuthSource != null && userInfo != null) {
            schemaRegistryClientConfigs.put("basic.auth.credentials.source", basicAuthSource);
            schemaRegistryClientConfigs.put("basic.auth.user.info", userInfo);
        }
        return new CachedSchemaRegistryClient(schemaRegistryUrl, identityMapCapacity, schemaRegistryClientConfigs);
    }
}
