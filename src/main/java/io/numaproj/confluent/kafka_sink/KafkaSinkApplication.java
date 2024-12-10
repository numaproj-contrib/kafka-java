package io.numaproj.confluent.kafka_sink;

import io.numaproj.numaflow.sinker.Server;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class KafkaSinkApplication {

    public static void main(String[] args) throws Exception {
		log.info("Starting Confluent Kafka sink application...");
		KafkaSinkApplication app = new KafkaSinkApplication();
		app.handler();
	}

	public void handler() throws Exception {
		ConfigurableApplicationContext applicationContext = new AnnotationConfigApplicationContext(KafkaSinkApplicationConfig.class);
		applicationContext.registerShutdownHook();
		Server server = applicationContext.getBean(Server.class);
		server.start();
	}
}
