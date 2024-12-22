package io.numaproj.kafka.consumer;

import com.google.common.annotations.VisibleForTesting;
import io.numaproj.kafka.common.CommonUtils;
import io.numaproj.numaflow.sourcer.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * KafkaSinker uses the schema defined in schema registry to parse, serialize and publish messages
 * to the target Kafka topic.
 */
@Slf4j
@Component
public class KafkaSourcer extends Sourcer {
  private final Worker worker;
  private final Admin admin;
  private Thread workerThread;

  // readTopicPartitionOffsetMap is used to keep track of the highest offsets read from the current
  // batch. The key is the topic:partition and the value is the highest offset read from the
  // partition. This map is used to validate the ack request.
  private Map<String, Long> readTopicPartitionOffsetMap;

  @Autowired
  public KafkaSourcer(Worker worker, Admin admin) {
    log.info("KafkaSourcer initialized with worker and admin configurations");
    this.worker = worker;
    this.admin = admin;
  }

  @PostConstruct
  public void startConsumer() throws Exception {
    log.info("Starting the Kafka consumer worker thread...");
    workerThread = new Thread(worker, "consumerWorkerThread");
    workerThread.start();
    log.info("Initializing Kafka sourcer server...");
    new Server(this).start();
  }

  /**
   * This method is added mainly to assist with unit tests
   *
   * @return boolean if the consumer worker thread is alive
   */
  boolean isWorkerThreadAlive() {
    return workerThread.isAlive();
  }

  /** Used in tests */
  @VisibleForTesting
  public void setReadTopicPartitionOffsetMap(Map<String, Long> readTopicPartitionOffsetMap) {
    this.readTopicPartitionOffsetMap = readTopicPartitionOffsetMap;
  }

  public void kill(Exception e) {
    log.error("Received kill signal, shutting down consumer worker", e);
    System.exit(100);
  }

  @Override
  public void read(ReadRequest request, OutputObserver observer) {
    // check if the consumer worker thread is still alive
    if (!isWorkerThreadAlive()) {
      log.error("Consumer worker thread is not alive, exiting...");
      kill(new RuntimeException("Consumer worker thread is not alive"));
    }

    long startTime;
    long remainingTime = request.getTimeout().toMillis();
    int j = 0;
    List<ConsumerRecord<String, GenericRecord>> consumerRecordList = null;
    readTopicPartitionOffsetMap = new HashMap<>();
    while (j < request.getCount()) {
      startTime = System.currentTimeMillis();
      // if the read request has timed out, break the loop
      if (remainingTime <= 0) {
        break;
      }
      try {
        consumerRecordList = worker.poll();
      } catch (InterruptedException e) {
        // Exit from the loop if the thread is interrupted
        kill(new RuntimeException(e));
      }
      if (consumerRecordList == null) {
        // update the remaining time
        remainingTime -= System.currentTimeMillis() - startTime;
        continue;
      }

      for (ConsumerRecord<String, GenericRecord> consumerRecord : consumerRecordList) {
        if (consumerRecord == null) {
          continue;
        }
        Map<String, String> kafkaHeaders = new HashMap<>();
        for (Header header : consumerRecord.headers()) {
          kafkaHeaders.put(header.key(), new String(header.value()));
        }
        // TODO - Do we need to add cluster ID to the offset value?
        // For now, it's probably good enough.
        String offsetValue = consumerRecord.topic() + ":" + consumerRecord.offset();
        byte[] payload = toJSON(consumerRecord.value());
        if (payload == null) {
          String errMsg = "Failed to convert the record to Json format: " + consumerRecord;
          log.error(errMsg);
          throw new RuntimeException(errMsg);
        }
        Message message =
            new Message(
                payload,
                new Offset(
                    offsetValue.getBytes(StandardCharsets.UTF_8), consumerRecord.partition()),
                Instant.ofEpochMilli(consumerRecord.timestamp()),
                kafkaHeaders);
        observer.send(message);

        String key =
            CommonUtils.getTopicPartitionKey(consumerRecord.topic(), consumerRecord.partition());
        if (readTopicPartitionOffsetMap.containsKey(key)) {
          if (readTopicPartitionOffsetMap.get(key) < consumerRecord.offset()) {
            readTopicPartitionOffsetMap.put(key, consumerRecord.offset());
          }
        } else {
          readTopicPartitionOffsetMap.put(key, consumerRecord.offset());
        }
        // FIXME - after we finish this for loop, j could be greater than request.getCount(),
        // meaning we are sending more messages than requested
        j++;
      }
      remainingTime -= System.currentTimeMillis() - startTime;
    }
    log.debug(
        "BatchRead summary: requested number of messages: {} number of messages sent: {} number of partitions: {} readTopicPartitionOffsetMap:{}",
        request.getCount(),
        j,
        readTopicPartitionOffsetMap.size(),
        readTopicPartitionOffsetMap);
  }

  @Override
  public void ack(AckRequest request) {
    Map<String, Long> topicPartitionOffsetMap = getPartitionToHighestOffsetMap(request);
    for (Map.Entry<String, Long> entry : topicPartitionOffsetMap.entrySet()) {
      if (readTopicPartitionOffsetMap == null
          || !readTopicPartitionOffsetMap.containsKey(entry.getKey())) {
        // TODO - emit error metrics
        log.error(
            "PANIC! THIS SHOULD NEVER HAPPEN. READ OFFSET MAP DOES NOT CONTAIN THE PARTITION ENTRY topic:partition:{}",
            entry.getKey());
      } else if (readTopicPartitionOffsetMap.get(entry.getKey()).longValue()
          != entry.getValue().longValue()) {
        // TODO - emit error metrics
        log.error(
            "PANIC! THIS SHOULD NEVER HAPPEN. READ AND ACK ARE NOT IN SYNC numa_ack:{} numa_read:{} topic:partition:{}",
            entry.getValue(),
            readTopicPartitionOffsetMap.get(entry.getKey()),
            entry.getKey());
      }
    }

    log.info(
        "No. of offsets in AckRequest:{}  topicPartitionOffsetList:{}",
        request.getOffsets().size(),
        topicPartitionOffsetMap);
    try {
      worker.commit();
    } catch (InterruptedException e) {
      // Exit from the loop if the thread is interrupted
      kill(new RuntimeException(e));
    }
  }

  private static Map<String, Long> getPartitionToHighestOffsetMap(AckRequest request) {
    Map<String, Long> topicPartitionOffsetMap = new HashMap<>();
    for (Offset offset : request.getOffsets()) {
      String[] topicOffset = new String(offset.getValue(), StandardCharsets.UTF_8).split(":");
      String topicName = topicOffset[0];
      long tmpOffset = Long.parseLong(topicOffset[1]);
      String key = CommonUtils.getTopicPartitionKey(topicName, offset.getPartitionId());
      if (topicPartitionOffsetMap.containsKey(key)) {
        if (topicPartitionOffsetMap.get(key) < tmpOffset) {
          topicPartitionOffsetMap.put(key, tmpOffset);
        }
      } else {
        topicPartitionOffsetMap.put(key, tmpOffset);
      }
    }
    return topicPartitionOffsetMap;
  }

  @Override
  public long getPending() {
    return admin.getPendingMessages();
  }

  @Override
  public List<Integer> getPartitions() {
    return worker.getPartitions();
  }

  // TODO - this is opinionated that we are transforming the record to JSON format
  // and send to the next Numaflow vertex. We need to make this more generic
  private byte[] toJSON(GenericRecord record) {
    Schema schema = record.getSchema();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (out) {
      DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
      datumWriter.write(record, encoder);
      encoder.flush();
    } catch (IOException e) {
      log.error("Failed to convert the record to JSON format: {}", record, e);
      return null;
    }
    return out.toByteArray();
  }
}
