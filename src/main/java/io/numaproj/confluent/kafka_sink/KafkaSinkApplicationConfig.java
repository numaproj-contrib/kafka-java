package io.numaproj.confluent.kafka_sink;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.numaproj.confluent.kafka_sink.config.KafkaSinkerConfig;
import io.numaproj.confluent.kafka_sink.sinker.KafkaSinker;
import io.numaproj.numaflow.sinker.Server;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Configuration
@Slf4j
@ComponentScan(basePackages = "io.numaproj.confluent.kafka_sink")
public class KafkaSinkApplicationConfig {

	private final Environment environment;

	public KafkaSinkApplicationConfig(Environment environment) {
		this.environment = environment;
	}

	@Bean
	public Server sinkServer(KafkaSinker kafkaSinker) {
		return new Server(kafkaSinker);
	}
	@Bean
	public KafkaSinkerConfig appConfig() {
		return new KafkaSinkerConfig("kafka-sink-topic");
	}
	@Bean
	public KafkaProducer kafkaProducer() throws IOException {
		Properties props = new Properties();
		ClassLoader classloader = Thread.currentThread().getContextClassLoader();
		InputStream is = classloader.getResourceAsStream("kafka-producer.config");
		props.load(is);
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		return new KafkaProducer<String, Payment>(props);
	}
}
