package io.numaproj.kafka.producer;

import io.numaproj.kafka.common.CommonUtils;
import io.numaproj.kafka.config.UserConfig;
import io.numaproj.kafka.schema.Registry;
import io.numaproj.numaflow.sinker.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
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

/** KafkaAvroSinker uses Avro schema to serialize and send the message */
@Slf4j
public class KafkaAvroSinker extends BaseKafkaSinker<GenericRecord> {
  private final Registry schemaRegistry;
  private final Schema schema;

  public KafkaAvroSinker(
      UserConfig userConfig,
      KafkaProducer<String, GenericRecord> producer,
      Registry schemaRegistry) {
    super(userConfig, producer);
    this.schemaRegistry = schemaRegistry;
    // Retrieve the schema from the schema registry
    this.schema =
        schemaRegistry.getAvroSchema(
            this.userConfig.getSchemaSubject(), this.userConfig.getSchemaVersion());
    if (schema == null) {
      String errMsg =
          "Failed to retrieve the schema for subject "
              + this.userConfig.getSchemaSubject()
              + ", version "
              + this.userConfig.getSchemaVersion();
      log.error(errMsg);
      throw new RuntimeException(errMsg);
    }
    log.info("Successfully retrieved the schema {}", schema.getFullName());
    log.info("KafkaAvroSinker initialized with use configurations: {}", userConfig);
  }

  public void startSinker() throws Exception {
    log.info("Initializing Kafka Avro sinker server...");
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

      GenericRecord avroGenericRecord;
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
