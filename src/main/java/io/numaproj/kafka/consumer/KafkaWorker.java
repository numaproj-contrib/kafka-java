package io.numaproj.kafka.consumer;

import io.numaproj.kafka.common.CommonUtils;
import io.numaproj.kafka.config.OnError;
import io.numaproj.kafka.config.UserConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;

/**
 * Consumes messages from the configured Kafka topics and commits offsets on demand. All Kafka
 * client access happens on this single worker thread; the sourcer thread interacts with it
 * exclusively through a one-at-a-time task queue, guaranteeing that poll, commit and shutdown never
 * run concurrently.
 *
 * <p>One consumer subscribes to every configured topic, so a batch is filled from whichever topics
 * have records and no topic is prioritized - the same design as Numaflow's builtin Kafka source.
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
  // Null in single-topic mode, where the bare Kafka partition is already the Numaflow partition ID.
  private final PartitionIdMapper partitionIdMapper;
  private final Set<String> configuredTopics;

  // A blocking queue used to hand tasks to the consumer thread. It ensures only one of the
  // tasks (POLL/COMMIT/SHUTDOWN) is performed at a time.
  private final BlockingQueue<OperationRequest> taskQueue = new LinkedBlockingQueue<>();
  // Signals the calling thread that the current operation has completed.
  private volatile CompletableFuture<Void> operationCompletion = new CompletableFuture<>();
  // Records polled from Kafka; volatile to ensure visibility across threads.
  private volatile List<ConsumerRecord<String, V>> consumerRecordList;
  // The last assignment seen by the worker thread. getPartitions() runs on the sourcer thread, and
  // KafkaConsumer throws ConcurrentModificationException if touched while this thread is polling,
  // so the assignment is snapshotted here instead of being read off-thread.
  private volatile Set<TopicPartition> currentAssignment = Set.of();
  // Only logged when it changes: getPartitions() is polled continuously.
  private volatile List<Integer> lastLoggedPartitions;

  public KafkaWorker(
      UserConfig userConfig,
      KafkaConsumer<String, V> consumer,
      SkippedRecordHandler skippedRecordHandler,
      PartitionIdMapper partitionIdMapper) {
    this.userConfig = userConfig;
    this.consumer = consumer;
    this.skippedRecordHandler = skippedRecordHandler;
    this.partitionIdMapper = partitionIdMapper;
    this.configuredTopics = Set.copyOf(userConfig.getTopicNames());
  }

  @Override
  public void run() {
    log.info("Consumer worker is running...");
    try {
      // Holds the one topic in single-topic mode, so both modes subscribe through this one call.
      log.info("Subscribing to topics: {}", userConfig.getTopicNames());
      consumer.subscribe(userConfig.getTopicNames());
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
   * Polls once for the given timeout. Under {@code onError: skip} a record that cannot be
   * deserialized is counted, logged and sought past, and the batch comes back empty; under {@code
   * onError: fail} the failure is rethrown.
   *
   * @throws RecordDeserializationException if a record could not be deserialized and {@code
   *     onError} is not {@code skip}
   */
  private void pollRecords(long timeoutMs) {
    List<ConsumerRecord<String, V>> polled = new ArrayList<>();
    try {
      for (ConsumerRecord<String, V> consumerRecord : consumer.poll(Duration.ofMillis(timeoutMs))) {
        log.debug(
            "consume:: topic:{} partition:{} offset:{} timestamp:{}",
            consumerRecord.topic(),
            consumerRecord.partition(),
            consumerRecord.offset(),
            Instant.ofEpochMilli(consumerRecord.timestamp()));
        polled.add(consumerRecord);
      }
      log.debug("number of messages polled: {}", polled.size());
      consumerRecordList = polled;
    } catch (RecordDeserializationException e) {
      if (userConfig.getOnError() != OnError.SKIP) {
        throw e;
      }
      skippedRecordHandler.handleSkipped(
          e.topicPartition().topic(),
          e.topicPartition().partition(),
          e.offset(),
          CommonUtils.sanitizeFailure(e.getCause() != null ? e.getCause() : e));
      // Kafka 4.0 caches the exception and does not advance nextFetchOffset, so a re-poll at the
      // same position rethrows without invoking the deserializer again. Seeking past the record
      // clears the cached fetch.
      consumer.seek(e.topicPartition(), e.offset() + 1);
      // Nothing to hand over: the next read resumes past the drop.
      consumerRecordList = List.of();
    }
    // Taken here so getPartitions() never touches the consumer from the sourcer thread. Being one
    // poll cycle stale does not matter: a partition is watermarked from the records it yields.
    currentAssignment = Set.copyOf(consumer.assignment());
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
   * Returns the Numaflow partition IDs of the partitions currently assigned to this consumer, which
   * is legitimately empty before the first poll completes and during a rebalance.
   *
   * <p>Deliberately not falling back to {@link
   * io.numaproj.numaflow.sourcer.Sourcer#defaultPartitions()}: it returns the replica index, which
   * under multi-topic is a real partition ID belonging to the alphabetically first topic.
   */
  public List<Integer> getPartitions() {
    List<Integer> partitions =
        currentAssignment.stream()
            // A topic rebalanced in but never configured has no ID reserved for it, so it is
            // dropped here rather than thrown on downstream.
            .filter(topicPartition -> configuredTopics.contains(topicPartition.topic()))
            .map(this::partitionId)
            .sorted()
            .collect(Collectors.toList());
    if (!partitions.equals(lastLoggedPartitions)) {
      log.debug("Partitions: {}", partitions);
      lastLoggedPartitions = partitions;
    }
    return partitions;
  }

  /**
   * The ID Numaflow watermarks this partition by: the bare Kafka partition in single-topic mode,
   * and one drawn from the topic's reserved range under multi-topic.
   */
  private int partitionId(TopicPartition topicPartition) {
    return partitionIdMapper == null
        ? topicPartition.partition()
        : partitionIdMapper.globalPartitionId(topicPartition);
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
