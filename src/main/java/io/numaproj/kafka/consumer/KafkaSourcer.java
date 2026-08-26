package io.numaproj.kafka.consumer;

import com.google.common.annotations.VisibleForTesting;
import io.numaproj.kafka.common.CommonUtils;
import io.numaproj.kafka.config.OnError;
import io.numaproj.kafka.config.UserConfig;
import io.numaproj.kafka.format.FormatException;
import io.numaproj.kafka.format.KafkaFormat;
import io.numaproj.kafka.metrics.SourceMetrics;
import io.numaproj.numaflow.sourcer.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;

/**
 * Numaflow {@link Sourcer} that reads messages from a Kafka topic. It is format agnostic: the value
 * type {@code V} and the mapping from Kafka value to downstream payload are supplied by an injected
 * {@link KafkaFormat}, so a single implementation serves Avro, JSON and raw byte-array sources.
 *
 * @param <V> the Kafka record value type
 */
@Slf4j
public class KafkaSourcer<V> extends Sourcer {

  /** Builds a Kafka consumer sized for the given Numaflow batch size. */
  @FunctionalInterface
  public interface ConsumerFactory<V> {
    KafkaConsumer<String, V> create(int batchSize) throws IOException;
  }

  private final UserConfig userConfig;
  private final Admin admin;
  private final KafkaFormat<V> format;
  private final ConsumerFactory<V> consumerFactory;
  private final SkippedRecordHandler skippedRecordHandler;

  // The worker and its thread are lazily initialized on the first read() call because the Numaflow
  // batch size (used to set max.poll.records) is not known until the first ReadRequest arrives.
  private KafkaWorker<V> worker;
  private Thread workerThread;

  // Tracks the highest offset read per topic:partition in the current batch, used to validate that
  // each ack request matches the preceding read request.
  private Map<String, Long> readTopicPartitionOffsetMap;

  public KafkaSourcer(
      UserConfig userConfig,
      Admin admin,
      KafkaFormat<V> format,
      ConsumerFactory<V> consumerFactory,
      SourceMetrics metrics) {
    this.userConfig = userConfig;
    this.admin = admin;
    this.format = format;
    this.consumerFactory = consumerFactory;
    // Shared with the worker so a drop is counted and logged identically at both read-path stages.
    this.skippedRecordHandler = new SkippedRecordHandler(metrics);
  }

  public void startConsumer() throws Exception {
    log.info("Initializing Kafka sourcer server...");
    new Server(this).start();
  }

  private synchronized void initWorkerIfNeeded(ReadRequest request) throws IOException {
    if (worker != null) {
      return;
    }
    int batchSize = (int) request.getCount();
    log.info(
        "Initializing consumer worker with batchSize={} timeoutMs={}",
        batchSize,
        request.getTimeout().toMillis());
    worker = new KafkaWorker<>(userConfig, consumerFactory.create(batchSize), skippedRecordHandler);
    workerThread = new Thread(worker, "consumerWorkerThread");
    workerThread.start();
  }

  @Override
  public void read(ReadRequest request, OutputObserver observer) {
    try {
      initWorkerIfNeeded(request);
    } catch (IOException e) {
      kill(new RuntimeException("Failed to initialize consumer worker", e));
      return;
    }

    if (!isWorkerThreadAlive()) {
      log.error("Consumer worker thread is not alive, exiting...");
      kill(new RuntimeException("Consumer worker thread is not alive"));
    }

    readTopicPartitionOffsetMap = new HashMap<>();
    List<ConsumerRecord<String, V>> consumerRecordList;
    try {
      consumerRecordList = worker.poll(request.getTimeout().toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      kill(new RuntimeException(e));
      return;
    } catch (RuntimeException e) {
      kill(e);
      return;
    }
    if (consumerRecordList == null) {
      return;
    }

    int sent = 0;
    try {
      for (ConsumerRecord<String, V> consumerRecord : consumerRecordList) {
        if (consumerRecord == null) {
          continue;
        }
        if (consumerRecord.value() == null) {
          // A Kafka tombstone: nothing to forward downstream.
          skippedRecordHandler.handleTombstone(consumerRecord.topic());
          continue;
        }
        Optional<byte[]> payload = toPayload(consumerRecord);
        if (payload.isEmpty()) {
          // Not tracked: Numaflow never received this record, so no ack will reference it. The
          // committed offset is unaffected - commitAsync() commits the consumer's position,
          // which is already past this record.
          continue;
        }
        observer.send(toMessage(consumerRecord, payload.get()));
        trackReadOffset(consumerRecord);
        sent++;
      }
    } catch (RuntimeException e) {
      kill(e);
      return;
    }
    log.debug(
        "BatchRead summary: requested:{} sent:{} partitions:{} readTopicPartitionOffsetMap:{}",
        request.getCount(),
        sent,
        readTopicPartitionOffsetMap.size(),
        readTopicPartitionOffsetMap);
  }

  /**
   * Converts a record's value to a payload, applying {@code onError} on failure.
   *
   * @return the payload, or empty if the record was dropped under {@code onError: skip}
   * @throws RuntimeException if the value cannot be converted and {@code onError} is not {@code
   *     skip}
   */
  private Optional<byte[]> toPayload(ConsumerRecord<String, V> consumerRecord) {
    try {
      return Optional.of(format.toPayload(consumerRecord.value()));
    } catch (FormatException e) {
      if (userConfig.getOnError() != OnError.SKIP) {
        throw new RuntimeException(
            "Failed to convert the record to a payload: "
                + SkippedRecordHandler.coordinates(
                    consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset()),
            e);
      }
      skippedRecordHandler.handleSkipped(
          consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), e);
      return Optional.empty();
    }
  }

  private static <V> Message toMessage(ConsumerRecord<String, V> consumerRecord, byte[] payload) {
    Map<String, String> kafkaHeaders = new HashMap<>();
    for (Header header : consumerRecord.headers()) {
      kafkaHeaders.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
    }
    // Set after the record's own headers so a producer-supplied header of the same name cannot
    // shadow the actual topic.
    kafkaHeaders.put(CommonUtils.SOURCE_TOPIC_NAME_HEADER, consumerRecord.topic());
    // TODO - Do we need to add cluster ID to the offset value? For now this is good enough.
    String offsetValue = consumerRecord.topic() + ":" + consumerRecord.offset();
    return new Message(
        payload,
        new Offset(offsetValue.getBytes(StandardCharsets.UTF_8), consumerRecord.partition()),
        Instant.ofEpochMilli(consumerRecord.timestamp()),
        kafkaHeaders);
  }

  private void trackReadOffset(ConsumerRecord<String, V> consumerRecord) {
    String key =
        CommonUtils.getTopicPartitionKey(consumerRecord.topic(), consumerRecord.partition());
    readTopicPartitionOffsetMap.merge(key, consumerRecord.offset(), Math::max);
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
        "No. of offsets in AckRequest:{} topicPartitionOffsetMap:{}",
        request.getOffsets().size(),
        topicPartitionOffsetMap);
    try {
      worker.commit();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      kill(new RuntimeException(e));
    } catch (RuntimeException e) {
      kill(e);
    }
  }

  private static Map<String, Long> getPartitionToHighestOffsetMap(AckRequest request) {
    Map<String, Long> topicPartitionOffsetMap = new HashMap<>();
    for (Offset offset : request.getOffsets()) {
      String[] topicOffset = new String(offset.getValue(), StandardCharsets.UTF_8).split(":");
      String key = CommonUtils.getTopicPartitionKey(topicOffset[0], offset.getPartitionId());
      long tmpOffset = Long.parseLong(topicOffset[1]);
      topicPartitionOffsetMap.merge(key, tmpOffset, Math::max);
    }
    return topicPartitionOffsetMap;
  }

  @Override
  public long getPending() {
    return admin.getPendingMessages();
  }

  @Override
  public List<Integer> getPartitions() {
    return worker == null ? List.of() : worker.getPartitions();
  }

  /** Logs the exception and exits with code 100, so Kubernetes restarts the pod as a crash. */
  public void kill(Exception e) {
    log.error("Received kill signal, shutting down the sourcer", e);
    System.exit(100);
  }

  /** Exposed to assist unit tests. */
  boolean isWorkerThreadAlive() {
    return workerThread != null && workerThread.isAlive();
  }

  @VisibleForTesting
  void setReadTopicPartitionOffsetMap(Map<String, Long> readTopicPartitionOffsetMap) {
    this.readTopicPartitionOffsetMap = readTopicPartitionOffsetMap;
  }

  /** Injects a pre-built worker directly, bypassing lazy init. Used in tests. */
  @VisibleForTesting
  void setWorker(KafkaWorker<V> worker, Thread thread) {
    this.worker = worker;
    this.workerThread = thread;
  }
}
