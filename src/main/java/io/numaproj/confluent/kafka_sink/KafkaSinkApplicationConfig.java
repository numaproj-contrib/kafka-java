package io.numaproj.confluent.kafka_sink;

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
import java.util.Properties;

@Configuration
@Slf4j
@ComponentScan(basePackages = "io.numaproj.confluent.kafka_sink")
public class KafkaSinkApplicationConfig {

    @Value("${producer.properties.path:NA}")
    private String producerPropertiesFilePath;

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
}
