package io.numaproj.kafka.producer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.numaproj.kafka.config.UserConfig;
import io.numaproj.kafka.format.AvroFormat;
import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.SinkerTestKit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Exercises the shared sinker flow through the Avro format. */
class KafkaSinkerTest {

  private static final String TOPIC = "test-topic";
  private static final String SCHEMA_JSON =
      "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";

  @SuppressWarnings("unchecked")
  private final KafkaProducer<String, org.apache.avro.generic.GenericRecord> producer =
      mock(KafkaProducer.class);

  private KafkaSinker<org.apache.avro.generic.GenericRecord> underTest;

  @BeforeEach
  void setUp() {
    UserConfig userConfig = mock(UserConfig.class);
    when(userConfig.getTopicName()).thenReturn(TOPIC);
    Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
    underTest = new KafkaSinker<>(userConfig, producer, AvroFormat.forSink(schema), null);
  }

  /** Builds a datum iterator from id -> json-value pairs. */
  private static SinkerTestKit.TestListIterator iterator(Map<String, String> idToValue) {
    SinkerTestKit.TestListIterator iterator = new SinkerTestKit.TestListIterator();
    idToValue.forEach(
        (id, value) ->
            iterator.addDatum(
                SinkerTestKit.TestDatum.builder().id(id).value(value.getBytes()).build()));
    return iterator;
  }

  private void producerSucceeds() {
    Future<RecordMetadata> future =
        CompletableFuture.completedFuture(
            new RecordMetadata(new TopicPartition(TOPIC, 1), 1, 1, 1, 1, 1));
    doReturn(future).when(producer).send(any(ProducerRecord.class));
  }

  private static Map<String, Boolean> successById(ResponseList responseList) {
    return responseList.getResponses().stream()
        .collect(Collectors.toMap(Response::getId, Response::getSuccess));
  }

  @Test
  void processMessages_allSucceed() {
    producerSucceeds();
    ResponseList result =
        underTest.processMessages(
            iterator(Map.of("1", "{\"name\":\"Michael\"}", "2", "{\"name\":\"Kobe\"}")));
    assertEquals(Map.of("1", true, "2", true), successById(result));
  }

  @Test
  void processMessages_invalidPayloadFails() {
    producerSucceeds();
    ResponseList result = underTest.processMessages(iterator(Map.of("1", "{\"age\":60}")));

    Response response = result.getResponses().getFirst();
    assertFalse(response.getSuccess());
    assertTrue(response.getErr().contains("Failed to prepare avro generic record"));
  }

  @Test
  void processMessages_whenSendFutureFails_thenResponseFails() {
    doReturn(CompletableFuture.failedFuture(new Exception("future error")))
        .when(producer)
        .send(any(ProducerRecord.class));

    ResponseList result = underTest.processMessages(iterator(Map.of("1", "{\"name\":\"Michael\"}")));

    assertEquals(Map.of("1", false), successById(result));
  }

  @Test
  void processMessages_usesKafkaKeyPrefix() {
    producerSucceeds();
    SinkerTestKit.TestListIterator iterator = new SinkerTestKit.TestListIterator();
    iterator.addDatum(
        SinkerTestKit.TestDatum.builder()
            .id("1")
            .value("{\"name\":\"Michael\"}".getBytes())
            .keys(new String[] {"KAFKA_KEY:custom-key"})
            .build());

    underTest.processMessages(iterator);

    assertEquals("custom-key", capturedRecord().key());
  }

  @Test
  void processMessages_generatesUuidKeyWhenNoPrefix() {
    producerSucceeds();
    SinkerTestKit.TestListIterator iterator = new SinkerTestKit.TestListIterator();
    iterator.addDatum(
        SinkerTestKit.TestDatum.builder()
            .id("1")
            .value("{\"name\":\"Michael\"}".getBytes())
            .keys(new String[] {"other-key"})
            .build());

    underTest.processMessages(iterator);

    assertEquals(36, capturedRecord().key().length());
  }

  @SuppressWarnings("unchecked")
  private ProducerRecord<String, org.apache.avro.generic.GenericRecord> capturedRecord() {
    ArgumentCaptor<ProducerRecord<String, org.apache.avro.generic.GenericRecord>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(producer).send(captor.capture());
    return captor.getValue();
  }
}
