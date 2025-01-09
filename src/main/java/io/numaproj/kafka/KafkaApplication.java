package io.numaproj.kafka;

import io.numaproj.kafka.consumer.*;
import io.numaproj.kafka.producer.BaseKafkaSinker;
import io.numaproj.kafka.producer.KafkaAvroSinker;
import io.numaproj.kafka.producer.KafkaByteArraySinker;
import io.numaproj.kafka.producer.KafkaJsonSinker;
import io.numaproj.kafka.schema.ConfluentRegistry;
import io.numaproj.kafka.schema.Registry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@Slf4j
@SpringBootApplication
@EnableConfigurationProperties
// TODO - there is a better way to group configurations for consumers and producers
// such that we don't need to explicitly exclude them here.
@ComponentScan(
    excludeFilters = {
      @ComponentScan.Filter(
          type = FilterType.ASSIGNABLE_TYPE,
          classes = {
            BaseKafkaSinker.class,
            KafkaAvroSinker.class,
            KafkaJsonSinker.class,
            KafkaByteArraySinker.class,
            AvroSourcer.class,
            ByteArraySourcer.class,
            Admin.class,
            AvroWorker.class,
            ByteArrayWorker.class,
            ConfluentRegistry.class,
            Registry.class
          })
    })
public class KafkaApplication {

  public static void main(String[] args) {
    // TODO - validate the arguments, cannot enable both consumer and producer
    log.info("Supplied arguments: {}", (Object) args);
    new SpringApplicationBuilder(KafkaApplication.class).run(args);
  }
}
