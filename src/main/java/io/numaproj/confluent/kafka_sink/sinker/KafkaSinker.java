package io.numaproj.confluent.kafka_sink.sinker;

import io.numaproj.confluent.kafka_sink.Payment;
import io.numaproj.confluent.kafka_sink.config.KafkaSinkerConfig;
import io.numaproj.numaflow.sinker.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaSinker extends Sinker implements DisposableBean {
    private static final String TOPIC = "transactions";
    private final String topicName;
    private final KafkaProducer<String, Payment> producer;

    @Autowired
    public KafkaSinker(
            KafkaSinkerConfig config,
            KafkaProducer<String, Payment> producer) {
        this.topicName = config.getTopicName();
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
                Payment payment = new Payment(msg, 1000.00d);
                final ProducerRecord<String, Payment> record = new ProducerRecord<>(TOPIC, payment.getId().toString(), payment);
                // TODO - this is an async call, should be sync.
                this.producer.send(record);
                responseListBuilder.addResponse(Response.responseOK(datum.getId()));
            } catch (Exception e) {
                log.error("Failed to process message", e);
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
