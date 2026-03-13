package io.numaproj.kafka.producer;

import io.numaproj.kafka.common.CommonUtils;
import io.numaproj.kafka.config.UserConfig;
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
 * KafkaByteArraySinker sends the raw messages without executing serialization. It is used when the
 * schemaType from {@link UserConfig} is set to raw, meaning the topic does not have a schema.
 */
@Slf4j
// TODO - this should be default when schemaType is not set, user should not have to set this when
// there is no schema associated with the topic
public class KafkaByteArraySinker extends BaseKafkaSinker<byte[]> {

  public KafkaByteArraySinker(UserConfig userConfig, KafkaProducer<String, byte[]> producer) {
    super(userConfig, producer);
    log.info("KafkaPlainSinker initialized with use configurations: {}", userConfig);
  }

  public void startSinker() throws Exception {
    log.info("Initializing Kafka byte array sinker server...");
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
      String key = CommonUtils.extractKafkaKey(datum.getKeys());
      if (key == null) {
        key = UUID.randomUUID().toString();
      }
      String msg = new String(datum.getValue());
      log.trace("Processing message with id: {}, payload: {}", datum.getId(), msg);

      ProducerRecord<String, byte[]> record =
          new ProducerRecord<>(this.userConfig.getTopicName(), key, datum.getValue());
      inflightTasks.put(datum.getId(), this.producer.send(record));
    }
    producer.flush();
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
    return responseListBuilder.build();
  }

  public void close() {
    log.info("Closing Kafka producer...");
    producer.close();
    log.info("Kafka producer closed.");
  }

  /** @deprecated Use {@link #close()} instead. */
  @Deprecated
  public void destroy() {
    close();
  }
}
