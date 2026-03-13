package io.numaproj.kafka.consumer;

import io.numaproj.kafka.config.UserConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/** Worker class that consumes messages from Kafka topic and commits offsets */
@Slf4j
public class AvroWorker implements Runnable {
  private final UserConfig userConfig;
  private final KafkaConsumer<String, GenericRecord> consumer;

  // A blocking queue to communicate with the consumer thread
  // It ensures only one of the tasks(POLL/COMMIT/SHUTDOWN) is performed at a time
  private final BlockingQueue<TaskType> taskQueue = new LinkedBlockingQueue<>();
  // CompletableFuture to signal the main thread
  private volatile CompletableFuture<Void> operationCompletion = new CompletableFuture<>();
  // List of messages consumed from kafka, volatile to ensure visibility across threads
  private volatile List<ConsumerRecord<String, GenericRecord>> consumerRecordList;

  public AvroWorker(UserConfig userConfig, KafkaConsumer<String, GenericRecord> consumer) {
    this.userConfig = userConfig;
    this.consumer = consumer;
  }

  @Override
  public void run() {
    log.info("Consumer worker is running...");
    try {
      String topicName = userConfig.getTopicName();
      consumer.subscribe(List.of(topicName));
      // TODO - make kafka poll timeout milliseconds configurable
      final Duration pollTimeout = Duration.ofMillis(100);
      boolean keepRunning = true;
      while (keepRunning) {
        TaskType taskType = taskQueue.take();
        switch (taskType) {
          case POLL -> {
            consumerRecordList = new ArrayList<>();
            final ConsumerRecords<String, GenericRecord> consumerRecords =
                consumer.poll(pollTimeout);
            for (final ConsumerRecord<String, GenericRecord> consumerRecord : consumerRecords) {
              if (consumerRecord.value() == null) {
                continue;
              }
              log.debug(
                  "consume:: partition:{}  offset:{} timestamp:{}",
                  consumerRecord.partition(),
                  consumerRecord.offset(),
                  Instant.ofEpochMilli(consumerRecord.timestamp()));
              consumerRecordList.add(consumerRecord);
            }
            log.debug("number of messages polled: {}", consumerRecordList.size());
            operationCompletion.complete(null);
          }
          case COMMIT -> {
            consumer.commitAsync(
                (offsets, exception) -> {
                  if (exception != null) {
                    log.error("error while committing offsets: Offsets:{}", offsets, exception);
                  } else {
                    log.debug("offsets committed: {}", offsets);
                  }
                });
            operationCompletion.complete(null);
          }
          case SHUTDOWN -> {
            log.info("shutting down the consumer");
            keepRunning = false;
          }
          default -> log.trace("wait for task from main thread:{}", taskType);
        }
      }
    } catch (Exception e) {
      log.error("error in consuming from kafka", e);
    } finally {
      consumer.close();
      operationCompletion.complete(null);
    }
  }

  /**
   * Sends signal to the consumer thread to start polling messages from kafka topic
   *
   * @return list of messages
   * @throws InterruptedException if the thread is interrupted
   */
  public List<ConsumerRecord<String, GenericRecord>> poll() throws InterruptedException {
    operationCompletion = new CompletableFuture<>();
    taskQueue.add(TaskType.POLL);
    try {
      operationCompletion.get();
    } catch (Exception e) {
      Thread.currentThread().interrupt();
      throw new InterruptedException(e.getMessage());
    }
    if (consumerRecordList == null) {
      return new ArrayList<>();
    }
    return new ArrayList<>(consumerRecordList);
  }

  /**
   * Sends signal to the consumer thread to commit offsets
   *
   * @throws InterruptedException if the thread is interrupted
   */
  public void commit() throws InterruptedException {
    operationCompletion = new CompletableFuture<>();
    taskQueue.add(TaskType.COMMIT);
    try {
      operationCompletion.get();
    } catch (Exception e) {
      Thread.currentThread().interrupt();
      throw new InterruptedException(e.getMessage());
    }
  }

  /**
   * @return list of partitions assigned to the consumer
   */
  public List<Integer> getPartitions() {
    List<Integer> partitions =
        consumer.assignment().stream()
            .filter(p -> p.topic().equals(userConfig.getTopicName()))
            .map(TopicPartition::partition)
            .collect(Collectors.toList());
    log.debug("Partitions: {}", partitions);
    return partitions;
  }

  public void shutdown() throws InterruptedException {
    if (consumer != null) {
      log.info("Consumer worker is shutting down...");
      operationCompletion = new CompletableFuture<>();
      taskQueue.add(TaskType.SHUTDOWN);
      try {
        operationCompletion.get();
      } catch (Exception e) {
        Thread.currentThread().interrupt();
        throw new InterruptedException(e.getMessage());
      }
      log.info("Consumer worker is closed");
    }
  }

  /** TaskType that the consumer worker thread can perform. */
  private enum TaskType {
    POLL,
    COMMIT,
    SHUTDOWN
  }
}
