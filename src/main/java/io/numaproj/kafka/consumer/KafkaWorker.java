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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Consumes messages from a Kafka topic and commits offsets on demand. All Kafka client access
 * happens on this single worker thread; the sourcer thread interacts with it exclusively through a
 * one-at-a-time task queue, guaranteeing that poll, commit and shutdown never run concurrently.
 *
 * <p>The worker is format agnostic: it is parameterized by the Kafka value type {@code V} and does
 * not interpret record values.
 *
 * @param <V> the Kafka record value type
 */
@Slf4j
public class KafkaWorker<V> implements Runnable {

  private final UserConfig userConfig;
  private final KafkaConsumer<String, V> consumer;

  // A blocking queue used to hand tasks to the consumer thread. It ensures only one of the
  // tasks (POLL/COMMIT/SHUTDOWN) is performed at a time.
  private final BlockingQueue<OperationRequest> taskQueue = new LinkedBlockingQueue<>();
  // Signals the calling thread that the current operation has completed.
  private volatile CompletableFuture<Void> operationCompletion = new CompletableFuture<>();
  // Records polled from Kafka; volatile to ensure visibility across threads.
  private volatile List<ConsumerRecord<String, V>> consumerRecordList;

  public KafkaWorker(UserConfig userConfig, KafkaConsumer<String, V> consumer) {
    this.userConfig = userConfig;
    this.consumer = consumer;
  }

  @Override
  public void run() {
    log.info("Consumer worker is running...");
    try {
      consumer.subscribe(List.of(userConfig.getTopicName()));
      boolean keepRunning = true;
      while (keepRunning) {
        OperationRequest request = taskQueue.take();
        try {
          switch (request.type) {
            case POLL -> pollRecords(request.timeoutMs);
            case COMMIT -> commitAsync();
            case SHUTDOWN -> {
              log.info("shutting down the consumer");
              keepRunning = false;
            }
          }
          operationCompletion.complete(null);
        } catch (Exception e) {
          log.error("error processing operation: {}", request.type, e);
          operationCompletion.completeExceptionally(e);
        }
      }
    } catch (Exception e) {
      log.error("error in consuming from kafka", e);
    } finally {
      consumer.close();
      operationCompletion.complete(null);
    }
  }

  private void pollRecords(long timeoutMs) {
    List<ConsumerRecord<String, V>> polled = new ArrayList<>();
    ConsumerRecords<String, V> consumerRecords = consumer.poll(Duration.ofMillis(timeoutMs));
    for (ConsumerRecord<String, V> consumerRecord : consumerRecords) {
      if (consumerRecord.value() == null) {
        continue;
      }
      log.debug(
          "consume:: partition:{} offset:{} timestamp:{}",
          consumerRecord.partition(),
          consumerRecord.offset(),
          Instant.ofEpochMilli(consumerRecord.timestamp()));
      polled.add(consumerRecord);
    }
    log.debug("number of messages polled: {}", polled.size());
    consumerRecordList = polled;
  }

  private void commitAsync() {
    consumer.commitAsync(
        (offsets, exception) -> {
          if (exception != null) {
            log.error("error while committing offsets: Offsets:{}", offsets, exception);
          } else {
            log.debug("offsets committed: {}", offsets);
          }
        });
  }

  /**
   * Requests the worker thread to poll messages and blocks until they are available.
   *
   * @return the list of records polled, never {@code null}
   * @throws InterruptedException if the calling thread is interrupted
   */
  public List<ConsumerRecord<String, V>> poll(long timeoutMs) throws InterruptedException {
    await(new OperationRequest(TaskType.POLL, timeoutMs));
    return consumerRecordList == null ? new ArrayList<>() : new ArrayList<>(consumerRecordList);
  }

  /**
   * Requests the worker thread to commit offsets and blocks until it completes.
   *
   * @throws InterruptedException if the calling thread is interrupted
   */
  public void commit() throws InterruptedException {
    await(new OperationRequest(TaskType.COMMIT));
  }

  /**
   * Requests the worker thread to shut down, closing the consumer, and blocks until it completes.
   *
   * @throws InterruptedException if the calling thread is interrupted
   */
  public void shutdown() throws InterruptedException {
    if (consumer == null) {
      return;
    }
    log.info("Consumer worker is shutting down...");
    await(new OperationRequest(TaskType.SHUTDOWN));
    log.info("Consumer worker is closed");
  }

  /**
   * @return the partitions of the configured topic currently assigned to this consumer
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

  /** Enqueues a request and blocks until the worker thread finishes processing it. */
  private void await(OperationRequest request) throws InterruptedException {
    operationCompletion = new CompletableFuture<>();
    taskQueue.add(request);
    try {
      operationCompletion.get();
    } catch (Exception e) {
      Thread.currentThread().interrupt();
      throw new InterruptedException(e.getMessage());
    }
  }

  /** A task for the worker thread to perform, with an optional poll timeout. */
  private record OperationRequest(TaskType type, long timeoutMs) {
    OperationRequest(TaskType type) {
      this(type, 0);
    }
  }

  private enum TaskType {
    POLL,
    COMMIT,
    SHUTDOWN
  }
}
