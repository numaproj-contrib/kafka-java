package io.numaproj.kafka.consumer;

import com.google.common.annotations.VisibleForTesting;
import io.numaproj.kafka.common.CommonUtils;
import io.numaproj.numaflow.sourcer.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * ByteArraySourcer is the implementation of the Numaflow Sourcer to read raw messages in byte array
 * format from Kafka
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "schemaType", havingValue = "raw")
public class ByteArraySourcer extends Sourcer {
  private final ByteArrayWorker worker;
  private final Admin admin;
  private Thread workerThread;

  // readTopicPartitionOffsetMap is used to keep track of the highest offsets read from the current
  // batch. The key is the topic:partition and the value is the highest offset read from the
  // partition. This map is used to validate the ack request, ensuring the ack request matches the
  // previous read request.
  private Map<String, Long> readTopicPartitionOffsetMap;

  @Autowired
  public ByteArraySourcer(ByteArrayWorker worker, Admin admin) {
    this.worker = worker;
    this.admin = admin;
  }

  @PostConstruct
  public void startConsumer() throws Exception {
    log.info("Starting the Kafka byte array consumer worker thread...");
    workerThread = new Thread(worker, "consumerWorkerThread");
    workerThread.start();
    log.info("Initializing Kafka byte array sourcer server...");
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
  void setReadTopicPartitionOffsetMap(Map<String, Long> readTopicPartitionOffsetMap) {
    this.readTopicPartitionOffsetMap = readTopicPartitionOffsetMap;
  }

  public void kill(Exception e) {
    log.error("Received kill signal, shutting down the sourcer", e);
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
    List<ConsumerRecord<String, byte[]>> consumerRecordList = null;
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

      for (ConsumerRecord<String, byte[]> consumerRecord : consumerRecordList) {
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
        Message message =
            new Message(
                consumerRecord.value(),
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

    log.debug(
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

  // This method is used to get the highest offset for each topic:partition from the AckRequest
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
}
