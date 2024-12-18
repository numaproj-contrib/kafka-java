package io.numaproj.kafka;

import io.numaproj.numaflow.sinker.Server;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@Slf4j
@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) throws Exception {
        log.info("Supplied arguments:{}", (Object) args);
        var ctx = new SpringApplicationBuilder(KafkaApplication.class)
                .run(args);
        Server server = ctx.getBean(Server.class);
        log.info("Starting the Kafka sink application...");
        server.start();
    }
}
