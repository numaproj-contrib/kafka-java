package io.numaproj.kafka;

import io.numaproj.numaflow.sourcer.Server;
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
        // TODO - conditionally load beans based on the numaflow.handler property
        Server server = ctx.getBean(Server.class);
        log.info("Starting the Kafka source application...");
        server.start();
    }
}
