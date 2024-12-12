package io.numaproj.confluent.kafka_sink;

import io.numaproj.numaflow.sinker.Server;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class KafkaSinkApplication {

    public static void main(String[] args) throws Exception {
        log.info("Supplied arguments:{}", (Object) args);
        var ctx = new org.springframework.boot.builder.SpringApplicationBuilder(KafkaSinkApplication.class)
                .run(args);
        Server server = ctx.getBean(Server.class);
        log.info("Starting the Kafka sink application...");
        server.start();
    }
}
