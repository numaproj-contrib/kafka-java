package io.numaproj.confluent.kafka_sink;

import io.numaproj.numaflow.sinker.Server;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@Slf4j
@SpringBootApplication
public class KafkaSinkApplication {

    public static void main(String[] args) throws Exception {
		log.info("supplied arguments:{}", (Object) args);
		var ctx = new org.springframework.boot.builder.SpringApplicationBuilder(KafkaSinkApplication.class)
				.run(args);
		// KafkaSinkApplication app = ctx.getBean(KafkaSinkApplication.class);
		log.info("Starting Confluent Kafka sink application...");
		Server server = ctx.getBean(Server.class);
		server.start();
		// app.handler();
	}

	public void handler() throws Exception {
		ConfigurableApplicationContext applicationContext = new AnnotationConfigApplicationContext(KafkaSinkApplicationConfig.class);
		applicationContext.registerShutdownHook();
		Server server = applicationContext.getBean(Server.class);
		server.start();
	}
}
