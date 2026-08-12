package io.numaproj.kafka.producer;

import io.numaproj.kafka.common.CommonUtils;
import io.numaproj.kafka.config.UserConfig;
import io.numaproj.kafka.format.FormatException;
import io.numaproj.kafka.format.KafkaFormat;
import io.numaproj.numaflow.sinker.*;
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

  public KafkaSinker(
      UserConfig userConfig, KafkaProducer<String, V> producer, KafkaFormat<V> format) {
    this.userConfig = userConfig;
    this.producer = producer;
    this.format = format;
    log.info("KafkaSinker initialized with user configurations: {}", userConfig);
  }

  /** Starts the sinker server and blocks until it terminates. */
  public void startSinker() throws Exception {
    log.info("Initializing Kafka sinker server...");
    Server server = new Server(this);
    server.start();
    server.awaitTermination();
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
      byte[] payload = datum.getValue();
      // The sink never produces a null or empty value. A tombstone is a deliberate act of the
      // system that owns the topic (a Debezium connector, say) — reaching one here means the
      // upstream vertex emitted nothing, which is a bug worth surfacing rather than writing a
      // delete marker on its behalf. The decrypting source still reads tombstones, since those
      // topics legitimately carry them.
      if (payload == null || payload.length == 0) {
        log.error("Refusing to produce a null or empty value for message with id: {}", datum.getId());
        responseListBuilder.addResponse(
            Response.responseFailure(
                datum.getId(), "the sink does not produce null or empty values"));
        continue;
      }

      V value;
      try {
        value = format.toRecord(payload);
      } catch (FormatException e) {
        log.error("Failed to convert message with id: {}", datum.getId(), e);
        responseListBuilder.addResponse(Response.responseFailure(datum.getId(), e.getMessage()));
        continue;
      }

      ProducerRecord<String, V> record =
          new ProducerRecord<>(userConfig.getTopicName(), resolveKey(datum), value);
      try {
        inflightTasks.put(datum.getId(), producer.send(record));
      } catch (Exception e) {
        // send() serializes the value on this thread before queueing the record, so a serializer
        // failure surfaces here rather than through the Future. With encryption on, that serializer
        // is the encrypting wrapper around the schema serializer (ProducerConfig#wrapWithEncryption)
        // — so schema resolution and encryption both fail into this catch. Fail this message only,
        // rather than letting the exception abandon the rest of the batch.
        log.error("Failed to send message with id: {}", datum.getId(), e);
        // toString, not getMessage: some exceptions carry a null message.
        responseListBuilder.addResponse(Response.responseFailure(datum.getId(), e.toString()));
      }
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

  public void close() {
    log.info("Closing Kafka producer...");
    producer.close();
    log.info("Kafka producer closed.");
  }
}
