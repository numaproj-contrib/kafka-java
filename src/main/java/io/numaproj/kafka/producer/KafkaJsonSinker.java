package io.numaproj.kafka.producer;

import io.numaproj.kafka.common.CommonUtils;
import io.numaproj.kafka.common.JsonValidator;
import io.numaproj.kafka.config.UserConfig;
import io.numaproj.kafka.schema.Registry;
import io.numaproj.numaflow.sinker.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/** KafkaJsonSinker uses json schema to serialize and send the message */
// TODO - JsonSinker can be merged with ByteArraySinker. The merged sinker will based on the schema
// information, serialize the message.
@Slf4j
public class KafkaJsonSinker extends BaseKafkaSinker<byte[]> {
  private final Registry schemaRegistry;
  private final String jsonSchema;

  public KafkaJsonSinker(
      UserConfig userConfig, KafkaProducer<String, byte[]> producer, Registry schemaRegistry) {
    super(userConfig, producer);

    this.schemaRegistry = schemaRegistry;
    this.jsonSchema =
        schemaRegistry.getJsonSchemaString(
            this.userConfig.getSchemaSubject(), this.userConfig.getSchemaVersion());
    if (Objects.equals(jsonSchema, "") || jsonSchema == null) {
      String errMsg =
          "Failed to retrieve the JSON schema, subject: "
              + this.userConfig.getSchemaSubject()
              + ", version: "
              + this.userConfig.getSchemaVersion();
      log.error(errMsg);
      throw new RuntimeException(errMsg);
    } else {
      log.info(
          "Successfully retrieved the JSON schema string for topic {}, schema string is {}",
          this.userConfig.getTopicName(),
          jsonSchema);
    }

    log.info("KafkaJsonSinker initialized with use configurations: {}", userConfig);
  }

  public void startSinker() throws Exception {
    log.info("Initializing Kafka JSON sinker server...");
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

      // validate the input data against Json schema.
      // the classic KafkaJsonSchemaSerializer requires a POJO being defined. It relies on
      // Java class annotations to generate and validate JSON schemas against stored schemas in the
      // Schema Registry.
      // Hence, we can't build a generic solution around that.
      // To build a generic one, we validate messages by ourselves by retrieving the schema
      // from the registry and use third party json validator to validate the raw input and then
      // directly use byte array serializer to send raw validated data to the topic.
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

  public void close() throws IOException {
    log.info("Closing Kafka producer and schema registry client...");
    producer.close();
    schemaRegistry.close();
    log.info("Kafka producer and schema registry client are closed.");
  }

  /** @deprecated Use {@link #close()} instead. */
  @Deprecated
  public void destroy() throws IOException, InterruptedException {
    close();
  }
}
