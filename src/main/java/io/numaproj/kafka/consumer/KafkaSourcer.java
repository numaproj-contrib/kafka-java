package io.numaproj.kafka.consumer;

import io.numaproj.kafka.config.UserConfig;
import io.numaproj.kafka.schema.Registry;
import io.numaproj.numaflow.sourcer.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * KafkaSinker uses the schema defined in schema registry to parse,
 * serialize and publish messages to the target Kafka topic.
 */
@Slf4j
@Component
public class KafkaSourcer extends Sourcer implements DisposableBean {

    private final String topicName;

    private final KafkaConsumer<String, GenericRecord> consumer;
    private final Registry schemaRegistry;
    private final AdminClient adminClient;

    @Autowired
    public KafkaSourcer(
            UserConfig config,
            KafkaConsumer<String, GenericRecord> consumer,
            Registry schemaRegistry,
            AdminClient adminClient) {
        this.topicName = config.getTopicName();
        this.consumer = consumer;
        this.schemaRegistry = schemaRegistry;
        this.adminClient = adminClient;
        log.info("KafkaConsumer initialized with topic name: {}", config.getTopicName());
        this.consumer.subscribe(List.of(this.topicName));
    }

    @Override
    public void read(ReadRequest request, OutputObserver observer) {
        // TODO - respect timeout
        ConsumerRecords<String, GenericRecord> records = consumer.poll(request.getTimeout());
        log.info("Received {} records from Kafka", records.count());
        records.forEach(record -> {
                    byte[] payload = toJSON(record.value());
                    if (payload == null) {
                        String errMsg = "Failed to serialize the record: " + record;
                        log.error(errMsg);
                        throw new RuntimeException(errMsg);
                    }
                    // TODO - let's match our builtin kafka offset format
                    String offsetValue = record.topic() + ":" + record.offset();
                    Message message = new Message(
                            payload,
                            new Offset(offsetValue.getBytes(StandardCharsets.UTF_8), record.partition()),
                            Instant.ofEpochMilli(record.timestamp())
                    );
                    observer.send(message);
                }
        );
    }

    @Override
    public void ack(AckRequest request) {

    }

    @Override
    public long getPending() {
        try {
            ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult =
                    adminClient.listConsumerGroupOffsets("group1");
            Map<TopicPartition, OffsetSpec> topicPartitionOffsetSpecMap = new HashMap<>();
            Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();
            topicPartitionOffsetAndMetadataMap.forEach((k, v) -> {
                if (topicName.equals(k.topic())) {
                    topicPartitionOffsetSpecMap.put(k, OffsetSpec.latest());
                }
            });
            // TODO - change to debug
            log.info("Topic Partition Offset MetaData Map: {}", topicPartitionOffsetAndMetadataMap);
            // Get latest Offset
            ListOffsetsResult listOffsetsResult = adminClient.listOffsets(topicPartitionOffsetSpecMap);
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionListOffsetsResultInfoMap = listOffsetsResult.all().get();
            long totalPending = topicPartitionListOffsetsResultInfoMap.keySet()
                    .stream().filter(k -> topicName.equals(k.topic()))
                    .map(k ->
                    {
                        OffsetAndMetadata currentOffset = topicPartitionOffsetAndMetadataMap.get(k);
                        ListOffsetsResult.ListOffsetsResultInfo latestOffset = topicPartitionListOffsetsResultInfoMap.get(k);
                        // TODO - change to debug
                        log.info("topic:{}, partition:{}, current offset:{}, latest offset:{}, pending count:{}", k.topic(), k.partition(),
                                currentOffset.offset(), latestOffset.offset(), latestOffset.offset() - currentOffset.offset());
                        return latestOffset.offset() - currentOffset.offset();
                    }).mapToLong(Long::longValue).sum();
            log.info("Total Pending Messages: {}", totalPending);
            return totalPending;
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to get pending messages", e);
            return -1;
        }
    }

    @Override
    public List<Integer> getPartitions() {
        List<Integer> partitions = consumer.assignment().stream().filter(p -> p.topic().equals(topicName)).map(TopicPartition::partition).collect(Collectors.toList());
        log.info("Partitions: {}", partitions);
        return partitions;
    }

    @Override
    public void destroy() throws IOException {
        log.info("Sending shutdown signal...");
        consumer.close();
        schemaRegistry.close();
        log.info("Kafka producer closed");
    }

    private byte[] toJSON(GenericRecord record) {
        Schema schema = record.getSchema();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (out) {
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
            datumWriter.write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            log.error("Failed to serialize the record to JSON format: {}", record, e);
            return null;
        }
        return out.toByteArray();
    }
}
