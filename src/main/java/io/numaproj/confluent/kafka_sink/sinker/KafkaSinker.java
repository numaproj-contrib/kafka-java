package io.numaproj.confluent.kafka_sink.sinker;

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
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
public class KafkaSinker extends Sinker implements DisposableBean {

    private final String topicName;
    private final String schema;
    private final KafkaProducer<String, GenericRecord> producer;

    @Autowired
    public KafkaSinker(
            KafkaSinkerConfig config,
            KafkaProducer<String, GenericRecord> producer) {
        log.info("KafkaSinker initialized with topic name: {}, schema: {}",
                config.getTopicName(), config.getSchema());
        this.topicName = config.getTopicName();
        this.schema = config.getSchema();
        this.producer = producer;
    }

    @Override
    public ResponseList processMessages(DatumIterator datumIterator) {
        ResponseList.ResponseListBuilder responseListBuilder = ResponseList.newBuilder();
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
            try {
                String msg = new String(datum.getValue());
                log.info("Received message: {}, headers - {}, topic name is - {}", msg, datum.getHeaders(), this.topicName);
                String key = UUID.randomUUID().toString();
                // writing original message to kafka
                log.info("Sending message to kafka: key - {}, value - {}", key, msg);
                Schema schema = new Schema.Parser().parse(this.schema);
                String jsonData = new String(datum.getValue());
                DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
                Decoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonData);
                GenericRecord result = reader.read(null, decoder);
                ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(this.topicName, key, result);
                // TODO - this is an async call, should be sync.
                this.producer.send(record);
                responseListBuilder.addResponse(Response.responseOK(datum.getId()));
            } catch (Exception e) {
                log.error("Failed to process message - {}", e.getMessage());
                responseListBuilder.addResponse(Response.responseFailure(
                        datum.getId(),
                        e.getMessage()));
            }
        }
        return responseListBuilder.build();
    }

    @Override
    public void destroy() {
        log.info("send shutdown signal");
        log.info("kafka producer closed");
    }
}
