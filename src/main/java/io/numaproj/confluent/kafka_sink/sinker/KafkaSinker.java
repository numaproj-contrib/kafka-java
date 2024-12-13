package io.numaproj.confluent.kafka_sink.sinker;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.numaproj.confluent.kafka_sink.config.KafkaSinkerConfig;
import io.numaproj.numaflow.sinker.*;
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

import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
public class KafkaSinker extends Sinker implements DisposableBean {

    private final String topicName;
    private final Schema schema;
    private final KafkaProducer<String, GenericRecord> producer;
    private final SchemaRegistryClient schemaRegistryClient;

    private AtomicBoolean isShutdown;
    private final CountDownLatch countDownLatch;

    @Autowired
    public KafkaSinker(
            KafkaSinkerConfig config,
            KafkaProducer<String, GenericRecord> producer,
            SchemaRegistryClient schemaRegistryClient) {
        // TODO - the instance variables are messy here, because they are dependent on each other. Clean it up to make more modular.
        this.topicName = config.getTopicName();
        this.producer = producer;
        this.schemaRegistryClient = schemaRegistryClient;
        this.schema = this.getSchemaForTopic(this.topicName);
        if (this.schema == null) {
            throw new RuntimeException("Failed to retrieve schema for topic " + this.topicName);
        }
        this.isShutdown = new AtomicBoolean(false);
        this.countDownLatch = new CountDownLatch(1);
        log.info("KafkaSinker initialized with topic name: {}, schema: {}",
                config.getTopicName(), this.schema);
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

            log.trace("Processing message with id: {}, payload: {}", datum.getId(), new String(datum.getValue()));
            String key = UUID.randomUUID().toString();
            String msg = new String(datum.getValue());
            GenericRecord avroGenericRecord;
            try {
                avroGenericRecord = prepareRecord(msg);
            } catch (EOFException e) {
                // FIXME - this is a workaround for a bug in numaflow where an extra empty message is sent at the end of the stream.
                // remove this check once the bug is fixed.
                log.info("If this one only happens once, then it was getting retried. EOFException while preparing avro generic record from JSON data: {}", msg);
                log.info("this data is: {}, {}, {}, {}, {}", datum.getId(), datum.getValue(), datum.getHeaders(), datum.getEventTime(), datum.getWatermark());
                continue;
            } catch (IOException e) {
                log.error("Failed to prepare avro generic record from JSON data: {}", msg, e);
                responseListBuilder.addResponse(Response.responseFailure(datum.getId(), e.getMessage()));
                continue;
            }
            // TODO - remove this log statement
            log.info("Sending message to kafka: key - {}, value - {}", key, msg);
            ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(this.topicName, key, avroGenericRecord);
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
                responseListBuilder.addResponse(Response.responseFailure(
                        entry.getKey(),
                        e.getMessage()));
            }
        }
        if (isShutdown.get()) {
            log.info("shutdown signal received");
            countDownLatch.countDown();
        }
        return responseListBuilder.build();
    }

    /**
     * Triggerred during shutdown by the Spring framework. Allows the {@link KafkaSinker#processMessages(DatumIterator)}
     * to complete in-flight requests and then shuts down.
     */
    @Override
    public void destroy() throws InterruptedException {
        log.info("Sending shutdown signal...");
        isShutdown = new AtomicBoolean(true);
        countDownLatch.await();
        log.info("Kafka producer closed");
    }

    private GenericRecord prepareRecord(String jsonData) throws IOException {
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonData);
        return reader.read(null, decoder);
    }

    private Schema getSchemaForTopic(String topicName) {
        try {
            // Retrieve the latest schema metadata for the {topicName}-value
            SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(topicName + "-value");
            // TODO - support other schema types. JSON, Protobuf etc.
            if (!Objects.equals(schemaMetadata.getSchemaType(), "AVRO")) {
                throw new RuntimeException("Schema type is not AVRO for topic {}." + topicName);
            }
            AvroSchema avroSchema = (AvroSchema) schemaRegistryClient.getSchemaById(schemaMetadata.getId());
            log.info("Retrieved schema for topic {}: {}", topicName, avroSchema.rawSchema());
            return avroSchema.rawSchema();
        } catch (IOException | RestClientException e) {
            log.error("Failed to retrieve schema for topic {}. {}", topicName, e.getMessage());
            return null;
        }
    }
}
