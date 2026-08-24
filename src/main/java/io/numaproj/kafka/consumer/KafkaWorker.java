package io.numaproj.kafka.consumer;

import io.numaproj.kafka.config.OnError;
import io.numaproj.kafka.config.UserConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;

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
  private final SkippedRecordHandler skippedRecordHandler;

  // A blocking queue used to hand tasks to the consumer thread. It ensures only one of the
  // tasks (POLL/COMMIT/SHUTDOWN) is performed at a time.
  private final BlockingQueue<OperationRequest> taskQueue = new LinkedBlockingQueue<>();
  // Signals the calling thread that the current operation has completed.
  private volatile CompletableFuture<Void> operationCompletion = new CompletableFuture<>();
  // Records polled from Kafka; volatile to ensure visibility across threads.
  private volatile List<ConsumerRecord<String, V>> consumerRecordList;

  public KafkaWorker(
      UserConfig userConfig,
      KafkaConsumer<String, V> consumer,
      SkippedRecordHandler skippedRecordHandler) {
    this.userConfig = userConfig;
    this.consumer = consumer;
    this.skippedRecordHandler = skippedRecordHandler;
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

  /**
   * Polls until either records arrive or the read deadline elapses. When {@code onError: skip} is
   * configured, records that cannot be deserialized are counted, logged, and skipped; the consumer
   * seeks past them so the buffered fetch is cleared and the next poll can advance. When {@code
   * onError: fail} is configured, the first deserialization failure is rethrown immediately.
   *
   * @throws RecordDeserializationException if a record could not be deserialized and {@code onError}
   *     is not {@code skip}
   */
  private void pollRecords(long timeoutMs) {
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (true) {
      List<ConsumerRecord<String, V>> polled = new ArrayList<>();
      try {
        for (ConsumerRecord<String, V> consumerRecord :
            consumer.poll(Duration.ofMillis(remaining(deadlineNanos)))) {
          log.debug(
              "consume:: partition:{} offset:{} timestamp:{}",
              consumerRecord.partition(),
              consumerRecord.offset(),
              Instant.ofEpochMilli(consumerRecord.timestamp()));
          polled.add(consumerRecord);
        }
        log.debug("number of messages polled: {}", polled.size());
        consumerRecordList = polled;
        return;
      } catch (RecordDeserializationException e) {
        if (userConfig.getOnError() != OnError.SKIP) {
          throw e;
        }
        skippedRecordHandler.handleSkipped(
            e.topicPartition().topic(),
            e.topicPartition().partition(),
            e.offset(),
            e.getCause() != null ? e.getCause() : e);
        // Kafka 4.0 caches the exception and does not advance nextFetchOffset, so a re-poll at the
        // same position rethrows without invoking the deserializer again. Seeking past the record
        // clears the cached fetch.
        consumer.seek(e.topicPartition(), e.offset() + 1);
        if (remaining(deadlineNanos) == 0) {
          // Read deadline spent; the next read cycle resumes past the drops.
          consumerRecordList = List.of();
          return;
        }
      }
    }
  }

  private static long remaining(long deadlineNanos) {
    return Math.max(0L, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));
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
   * @throws RecordDeserializationException if a record could not be deserialized and {@code
   *     onError} is not {@code skip}
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

  /**
   * Enqueues a request and blocks until the worker thread finishes processing it.
   *
   * @throws InterruptedException if the calling thread is genuinely interrupted while waiting
   */
  private void await(OperationRequest request) throws InterruptedException {
    operationCompletion = new CompletableFuture<>();
    taskQueue.add(request);
    try {
      operationCompletion.get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      throw cause instanceof RuntimeException runtime
          ? runtime
          : new RuntimeException("Kafka " + request.type() + " operation failed", cause);
    }
  }

  /** A task for the worker thread to perform, with the poll timeout it needs. */
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
