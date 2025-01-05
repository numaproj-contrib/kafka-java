package io.numaproj.kafka.producer;

import io.numaproj.kafka.config.UserConfig;
import io.numaproj.numaflow.sinker.*;
import java.util.HashMap;
import java.util.Map;
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

/**
 * KafkaPlainSinker sends the raw messages without executing serialization. It is used when the
 * schemaType from {@link UserConfig} is not set, meaning the topic does not have a schema.
 *
 * <p>If there is a schema associated with the topic, this sinker will send the raw message even
 * when the message doesn't match the schema. There is no client side validation for the message.
 *
 * <p>TODO - consider consulting the schema registry first and throw an error if a schema is found.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "schemaType", matchIfMissing = true)
public class KafkaPlainSinker extends BaseKafkaSinker<byte[]> {
  private AtomicBoolean isShutdown;
  private final CountDownLatch countDownLatch;

  @Autowired
  public KafkaPlainSinker(UserConfig userConfig, KafkaProducer<String, byte[]> producer) {
    super(userConfig, producer);
    this.isShutdown = new AtomicBoolean(false);
    this.countDownLatch = new CountDownLatch(1);
    log.info("KafkaPlainSinker initialized with use configurations: {}", userConfig);
  }

  @PostConstruct
  public void startSinker() throws Exception {
    log.info("Initializing Kafka plain sinker server...");
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
   * KafkaPlainSinker#processMessages(DatumIterator)} to complete in-flight requests and then shuts
   * down.
   */
  @Override
  public void destroy() throws InterruptedException {
    log.info("Sending shutdown signal...");
    isShutdown = new AtomicBoolean(true);
    countDownLatch.await();
    producer.close();
    log.info("Kafka producer and schema registry client are closed.");
  }
}
