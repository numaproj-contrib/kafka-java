package io.numaproj.kafka.producer;

import io.numaproj.kafka.common.CommonUtils;
import io.numaproj.kafka.config.UserConfig;
import io.numaproj.kafka.format.FormatException;
import io.numaproj.kafka.format.KafkaFormat;
import io.numaproj.kafka.schema.Registry;
import io.numaproj.numaflow.sinker.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Numaflow {@link Sinker} that publishes messages to a Kafka topic. It is format agnostic: the value
 * type {@code V} and the mapping from incoming payload to Kafka value are supplied by an injected
 * {@link KafkaFormat}, so a single implementation serves Avro, JSON and raw byte-array sinks.
 *
 * @param <V> the Kafka record value type
 */
@Slf4j
public class KafkaSinker<V> extends Sinker {

  private final UserConfig userConfig;
  private final KafkaProducer<String, V> producer;
  private final KafkaFormat<V> format;
  // Optional schema registry, present only for schema-backed formats; closed on shutdown.
  private final Registry schemaRegistry;

  public KafkaSinker(
      UserConfig userConfig,
      KafkaProducer<String, V> producer,
      KafkaFormat<V> format,
      Registry schemaRegistry) {
    this.userConfig = userConfig;
    this.producer = producer;
    this.format = format;
    this.schemaRegistry = schemaRegistry;
    log.info("KafkaSinker initialized with user configurations: {}", userConfig);
  }

  public void startSinker() throws Exception {
    log.info("Initializing Kafka sinker server...");
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
      // A null datum means the iterator is closed.
      if (datum == null) {
        break;
      }

      log.trace("Processing message with id: {}", datum.getId());
      V value;
      try {
        value = format.toRecord(datum.getValue());
      } catch (FormatException e) {
        log.error("Failed to convert message with id: {}", datum.getId(), e);
        responseListBuilder.addResponse(Response.responseFailure(datum.getId(), e.getMessage()));
        continue;
      }

      ProducerRecord<String, V> record =
          new ProducerRecord<>(userConfig.getTopicName(), resolveKey(datum), value);
      inflightTasks.put(datum.getId(), producer.send(record));
    }

    producer.flush();
    log.debug("Number of messages inflight to the topic is {}", inflightTasks.size());
    return awaitResponses(inflightTasks, responseListBuilder);
  }

  private static String resolveKey(Datum datum) {
    String key = CommonUtils.extractKafkaKey(datum.getKeys());
    return key != null ? key : UUID.randomUUID().toString();
  }

  private ResponseList awaitResponses(
      Map<String, Future<RecordMetadata>> inflightTasks,
      ResponseList.ResponseListBuilder responseListBuilder) {
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
    return responseListBuilder.build();
  }

  public void close() throws IOException {
    log.info("Closing Kafka producer...");
    producer.close();
    if (schemaRegistry != null) {
      schemaRegistry.close();
    }
    log.info("Kafka producer closed.");
  }
}
