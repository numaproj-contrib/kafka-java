package io.numaproj.kafka.producer;

import io.numaproj.kafka.config.UserConfig;
import io.numaproj.kafka.schema.Registry;
import io.numaproj.numaflow.sinker.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * KafkaSinker validates the input message against the schema of target topic and sends the message
 */
@Slf4j
@Component
public class KafkaSinker extends Sinker implements DisposableBean {
  private final UserConfig userConfig;
  private final KafkaProducer<String, GenericRecord> producer;
  private final Registry schemaRegistry;

  private AtomicBoolean isShutdown;
  private final CountDownLatch countDownLatch;

  @Autowired
  public KafkaSinker(
      UserConfig userConfig,
      KafkaProducer<String, GenericRecord> producer,
      Registry schemaRegistry) {
    this.userConfig = userConfig;
    this.producer = producer;
    this.schemaRegistry = schemaRegistry;
    this.isShutdown = new AtomicBoolean(false);
    this.countDownLatch = new CountDownLatch(1);
    log.info("KafkaSinker initialized with use configurations: {}", userConfig);
  }

  @PostConstruct
  public void startProducer() throws Exception {
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
      // null means the iterator is closed, so we break the loop
      if (datum == null) {
        break;
      }

      String key = UUID.randomUUID().toString();
      String msg = new String(datum.getValue());
      log.trace("Processing message with id: {}, payload: {}", datum.getId(), msg);

      GenericRecord avroGenericRecord;
      // TODO - assuming single topic, we don't need to fetch the schema for each message
      // see if there is any performance improvement by fetching the schema once.
      // currently sink can only do ~20 messages per second (2 pods 6 partitions)
      Schema schema = schemaRegistry.getAvroSchema(this.userConfig.getTopicName());
      if (schema == null) {
        // TODO - support retrieving versioned schema
        String errMsg =
            "Failed to retrieve the latest schema for topic " + this.userConfig.getTopicName();
        log.error(errMsg);
        responseListBuilder.addResponse(Response.responseFailure(datum.getId(), errMsg));
        continue;
      }

      try {
        // FIXME - this assumes the input data is in json format
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, msg);
        avroGenericRecord = reader.read(null, decoder);
      } catch (Exception e) {
        String errMsg = "Failed to prepare avro generic record " + e;
        log.error(errMsg);
        responseListBuilder.addResponse(Response.responseFailure(datum.getId(), errMsg));
        continue;
      }
      ProducerRecord<String, GenericRecord> record =
          new ProducerRecord<>(this.userConfig.getTopicName(), key, avroGenericRecord);
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
   * KafkaSinker#processMessages(DatumIterator)} to complete in-flight requests and then shuts down.
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
