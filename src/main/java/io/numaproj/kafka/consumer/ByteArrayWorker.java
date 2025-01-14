package io.numaproj.kafka.consumer;

import io.numaproj.kafka.config.UserConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

/** Worker class that consumes messages from Kafka topic and commits offsets */
@Slf4j
@Component
// TODO - single place to hold the value "raw"
@ConditionalOnExpression("'${schemaType}'.equals('json') or '${schemaType}'.equals('raw')")
public class ByteArrayWorker implements Runnable, DisposableBean {
  private final UserConfig userConfig;
  private final KafkaConsumer<String, byte[]> consumer;

  // A blocking queue to communicate with the consumer thread
  // It ensures only one of the tasks(POLL/COMMIT/SHUTDOWN) is performed at a time
  private static final BlockingQueue<TaskType> taskQueue = new LinkedBlockingQueue<>();
  // CountDownLatch to signal the main thread
  private static CountDownLatch countdownLatchSignalMainThread = new CountDownLatch(1);
  // List of messages consumed from kafka, volatile to ensure visibility across threads
  private volatile List<ConsumerRecord<String, byte[]>> consumerRecordList;

  @Autowired
  public ByteArrayWorker(UserConfig userConfig, KafkaConsumer<String, byte[]> consumer) {
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
            final ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(pollTimeout);
            for (final ConsumerRecord<String, byte[]> consumerRecord : consumerRecords) {
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
            countdownLatchSignalMainThread.countDown();
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
            countdownLatchSignalMainThread.countDown();
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
      countdownLatchSignalMainThread.countDown();
    }
  }

  /**
   * Sends signal to the consumer thread to start polling messages from kafka topic
   *
   * @return list of messages
   * @throws InterruptedException if the thread is interrupted
   */
  public List<ConsumerRecord<String, byte[]>> poll() throws InterruptedException {
    countdownLatchSignalMainThread = new CountDownLatch(1);
    taskQueue.add(TaskType.POLL);
    countdownLatchSignalMainThread.await();
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
    countdownLatchSignalMainThread = new CountDownLatch(1);
    taskQueue.add(TaskType.COMMIT);
    countdownLatchSignalMainThread.await();
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
    log.info("Partitions: {}", partitions);
    return partitions;
  }

  @Override
  public void destroy() throws InterruptedException {
    if (consumer != null) {
      log.info("Consumer worker is shutting down...");
      countdownLatchSignalMainThread = new CountDownLatch(1);
      taskQueue.add(TaskType.SHUTDOWN);
      countdownLatchSignalMainThread.await();
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
