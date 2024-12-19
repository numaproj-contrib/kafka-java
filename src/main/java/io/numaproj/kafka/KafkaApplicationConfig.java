package io.numaproj.kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.numaproj.kafka.consumer.KafkaSourcer;
import io.numaproj.kafka.producer.KafkaSinker;
import io.numaproj.kafka.schema.ConfluentRegistry;
import io.numaproj.kafka.schema.Registry;
import io.numaproj.numaflow.sinker.Server;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

/**
 * Defines all the beans used as part of this project
 */
@Slf4j
@Configuration
@ComponentScan(basePackages = "io.numaproj.kafka")
public class KafkaApplicationConfig {

    @Value("${producer.properties.path:NA}")
    private String producerPropertiesFilePath;

    @Value("${schema.registry.properties.path:NA}")
    private String schemaRegistryPropertiesFilePath;

    @Value("${consumer.properties.path:NA}")
    private String consumerPropertiesFilePath;

    // package-private constructor. this is for unit test only.
    KafkaApplicationConfig(
            @Value("${producer.properties.path:NA}") String producerPropertiesFilePath,
            @Value("${schema.registry.properties.path:NA}") String schemaRegistryPropertiesFilePath,
            @Value("${consumer.properties.path:NA}") String consumerPropertiesFilePath) {
        this.producerPropertiesFilePath = producerPropertiesFilePath;
        this.schemaRegistryPropertiesFilePath = schemaRegistryPropertiesFilePath;
        this.consumerPropertiesFilePath = consumerPropertiesFilePath;
    }

    @Bean
    public Server sinkServer(KafkaSinker kafkaSinker) {
        return new Server(kafkaSinker);
    }

    @Bean
    public io.numaproj.numaflow.sourcer.Server sourceServer(KafkaSourcer kafkaSourcer) {
        return new io.numaproj.numaflow.sourcer.Server(kafkaSourcer);
    }

    @Bean
    public KafkaProducer<String, GenericRecord> kafkaProducer() throws IOException {
        log.info("Instantiating the Kafka producer from the producer properties file path: {}", this.producerPropertiesFilePath);
        Properties props = new Properties();
        InputStream is = new FileInputStream(this.producerPropertiesFilePath);
        props.load(is);
        log.info("Kafka producer props read from user input ConfigMap: {}", props);
        return new KafkaProducer<>(props);
    }

    @Bean
    public KafkaConsumer<String, GenericRecord> kafkaConsumer() throws IOException {
        log.info("Instantiating the Kafka consumer from the consumer properties file path: {}", this.consumerPropertiesFilePath);
        Properties props = new Properties();
        InputStream is = new FileInputStream(this.consumerPropertiesFilePath);
        props.load(is);
        // TODOo - remove hardcoding
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");

        log.info("Kafka consumer props read from user input ConfigMap: {}", props);
        return new KafkaConsumer<>(props);
    }

    // AdminClient is used to retrieve the number of pending messages.
    // It is only used by the sourcer.
    // FIXME - currently sharing the consumer properties file path with kafka consumer client.
    // There has to be a better way to do this, since admin client should be able to serve both consumer and producer,
    // and it does not need all the properties that consumer client needs.
    @Bean
    public AdminClient kafkaAdminClient() throws IOException {
        Properties props = new Properties();
        InputStream is = new FileInputStream(this.consumerPropertiesFilePath);
        props.load(is);
        log.info("Kafka admin client props read from consumer properties: {}", props);
        return KafkaAdminClient.create(props);
    }

    @Bean
    public SchemaRegistryClient schemaRegistryClient() throws IOException {
        log.info("Instantiating the Kafka schema registry client from the schema registry properties file path: {}", this.schemaRegistryPropertiesFilePath);
        Properties props = new Properties();
        InputStream is = new FileInputStream(this.schemaRegistryPropertiesFilePath);
        props.load(is);
        String schemaRegistryUrl = props.getProperty("schema.registry.url");
        int identityMapCapacity = Integer.parseInt(props.getProperty("schema.registry.identity.map.capacity", "100")); // Default to 100 if not specified
        Map<String, String> schemaRegistryClientConfigs = new HashMap<>();
        for (String key : props.stringPropertyNames()) {
            schemaRegistryClientConfigs.put(key, props.getProperty(key));
        }
        return new CachedSchemaRegistryClient(schemaRegistryUrl, identityMapCapacity, schemaRegistryClientConfigs);
    }

    @Bean
    public Registry schemaRegistry(SchemaRegistryClient schemaRegistryClient) {
        return new ConfluentRegistry(schemaRegistryClient);
    }
}
