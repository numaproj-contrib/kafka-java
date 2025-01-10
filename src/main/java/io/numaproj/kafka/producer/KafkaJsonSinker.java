package io.numaproj.kafka.producer;

import io.numaproj.kafka.common.JsonValidator;
import io.numaproj.kafka.config.UserConfig;
import io.numaproj.kafka.schema.Registry;
import io.numaproj.numaflow.sinker.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** KafkaJsonSinker uses json schema to serialize and send the message */
// TODO - JsonSinker can be merged with ByteArraySinker. The merged sinker will based on the schema
// information, serialize the message.
@Slf4j
@Component
@ConditionalOnProperty(name = "schemaType", havingValue = "json")
public class KafkaJsonSinker extends BaseKafkaSinker<byte[]> {
  private final Registry schemaRegistry;
  private final String jsonSchema;

  private AtomicBoolean isShutdown;
  private final CountDownLatch countDownLatch;

  @Autowired
  public KafkaJsonSinker(
      UserConfig userConfig, KafkaProducer<String, byte[]> producer, Registry schemaRegistry) {
    super(userConfig, producer);

    this.schemaRegistry = schemaRegistry;
    this.jsonSchema = schemaRegistry.getJsonSchemaString(this.userConfig.getTopicName());
    if (Objects.equals(jsonSchema, "") || jsonSchema == null) {
      log.error(
          "Failed to retrieve the latest json schema string for topic {}",
          this.userConfig.getTopicName());
      throw new RuntimeException("Failed to retrieve the latest json schema string for topic");
    } else {
      log.info(
          "Retrieved the latest json schema string for topic {}, schema is {}",
          this.userConfig.getTopicName(),
          jsonSchema);
    }

    this.isShutdown = new AtomicBoolean(false);
    this.countDownLatch = new CountDownLatch(1);

    log.info("KafkaJsonSinker initialized with use configurations: {}", userConfig);
  }

  @PostConstruct
  public void startSinker() throws Exception {
    log.info("Initializing Kafka json sinker server...");
    new Server(this).start();
  }

  @Override
  public ResponseList processMessages(DatumIterator datumIterator) {
    ResponseList.ResponseListBuilder responseListBuilder = ResponseList.newBuilder();
    Map<String, Future<RecordMetadata>> inflightTasks = new HashMap<>();
    while (true) {
      Datum datum;
      try {
        datum = datumIterator.next();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        continue;
      }
      // null means the iterator is closed, so we break the loop
      if (datum == null) {
        break;
      }
      String key = UUID.randomUUID().toString();
      String msg = new String(datum.getValue());
      log.trace("Processing message with id: {}, payload: {}", datum.getId(), msg);

      // TODO - validate the input data against Json schema
      // the classic KafkaJsonSchemaSerializer requires a POJO being defined. It relies on
      // Java class annotations to generate and validate JSON schemas against stored schemas in the
      // Schema Registry.
      // Hence, we can’t build a generic solution around that.
      // To build a generic one, we need to validate messages by ourselves by retrieving the schema
      // from the registry and use third party json validator to validate the raw input and then
      // directly use string serializer to send raw validated string to the topic.
      if (!JsonValidator.validate(jsonSchema, datum.getValue())) {
        log.error("Failed to validate the message with id: {}, message: {}", datum.getId(), msg);
        responseListBuilder.addResponse(
            Response.responseFailure(datum.getId(), "Failed to validate the message"));
        continue;
      }

      ProducerRecord<String, byte[]> record =
          new ProducerRecord<>(this.userConfig.getTopicName(), key, datum.getValue());
      inflightTasks.put(datum.getId(), this.producer.send(record));
    }
    log.debug("Number of messages inflight to the topic is {}", inflightTasks.size());
    for (Map.Entry<String, Future<RecordMetadata>> entry : inflightTasks.entrySet()) {
      try {
        entry.getValue().get();
        responseListBuilder.addResponse(Response.responseOK(entry.getKey()));
        log.trace("Successfully processed message with id: {}", entry.getKey());
      } catch (Exception e) {
        log.error("Failed to process message with id: {}", entry.getKey(), e);
        responseListBuilder.addResponse(Response.responseFailure(entry.getKey(), e.getMessage()));
      }
    }
    if (isShutdown.get()) {
      log.info("shutdown signal received");
      countDownLatch.countDown();
    }
    return responseListBuilder.build();
  }

  /**
   * Triggerred during shutdown by the Spring framework. Allows the {@link
   * KafkaJsonSinker#processMessages(DatumIterator)} to complete in-flight requests and then shuts
   * down.
   */
  @Override
  public void destroy() throws InterruptedException, IOException {
    log.info("Sending shutdown signal...");
    isShutdown = new AtomicBoolean(true);
    countDownLatch.await();
    producer.close();
    schemaRegistry.close();
    log.info("Kafka producer and schema registry client are closed.");
  }
}
