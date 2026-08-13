package io.numaproj.kafka.producer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.numaproj.kafka.config.UserConfig;
import io.numaproj.kafka.format.AvroFormat;
import io.numaproj.kafka.format.ByteArrayFormat;
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
import org.apache.kafka.common.errors.SerializationException;
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
    underTest = new KafkaSinker<>(userConfig, producer, AvroFormat.forSink(schema));
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
  void processMessages_nullValueFailsAvroConversionAndIsNotProduced() {
    // An empty payload is not a valid Avro record, so the avro sink fails it in conversion. A raw
    // sink produces it as a tombstone — see rawSinkProducesAnEmptyPayloadAsATombstone.
    SinkerTestKit.TestListIterator iterator = new SinkerTestKit.TestListIterator();
    iterator.addDatum(SinkerTestKit.TestDatum.builder().id("1").value(null).build());

    ResponseList result = underTest.processMessages(iterator);

    Response response = result.getResponses().getFirst();
    assertFalse(response.getSuccess());
    assertTrue(response.getErr().contains("Failed to prepare avro generic record"));
    verify(producer, never()).send(any(ProducerRecord.class));
  }

  @Test
  void processMessages_emptyValueFailsAvroConversionAndIsNotProduced() {
    SinkerTestKit.TestListIterator iterator = new SinkerTestKit.TestListIterator();
    iterator.addDatum(SinkerTestKit.TestDatum.builder().id("1").value(new byte[0]).build());

    ResponseList result = underTest.processMessages(iterator);

    Response response = result.getResponses().getFirst();
    assertFalse(response.getSuccess());
    assertTrue(response.getErr().contains("Failed to prepare avro generic record"));
    verify(producer, never()).send(any(ProducerRecord.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void rawSinkProducesAnEmptyPayloadAsATombstone() {
    // The raw sink has no schema to violate: an empty payload is a tombstone, and writing one is the
    // pipeline's call, not this sink's.
    KafkaProducer<String, byte[]> rawProducer = mock(KafkaProducer.class);
    doReturn(
            CompletableFuture.completedFuture(
                new RecordMetadata(new TopicPartition(TOPIC, 1), 1, 1, 1, 1, 1)))
        .when(rawProducer)
        .send(any(ProducerRecord.class));
    UserConfig userConfig = mock(UserConfig.class);
    when(userConfig.getTopicName()).thenReturn(TOPIC);
    KafkaSinker<byte[]> rawSinker =
        new KafkaSinker<>(userConfig, rawProducer, new ByteArrayFormat());

    SinkerTestKit.TestListIterator iterator = new SinkerTestKit.TestListIterator();
    iterator.addDatum(SinkerTestKit.TestDatum.builder().id("1").value(null).build());

    ResponseList result = rawSinker.processMessages(iterator);

    assertEquals(Map.of("1", true), successById(result));
    ArgumentCaptor<ProducerRecord<String, byte[]>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(rawProducer).send(captor.capture());
    assertNull(captor.getValue().value());
  }

  @Test
  void processMessages_failedValueDoesNotAbandonTheRestOfTheBatch() {
    producerSucceeds();
    SinkerTestKit.TestListIterator iterator = new SinkerTestKit.TestListIterator();
    iterator.addDatum(SinkerTestKit.TestDatum.builder().id("1").value(new byte[0]).build());
    iterator.addDatum(
        SinkerTestKit.TestDatum.builder().id("2").value("{\"name\":\"Kobe\"}".getBytes()).build());

    ResponseList result = underTest.processMessages(iterator);

    assertEquals(Map.of("1", false, "2", true), successById(result));
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
  void processMessages_whenSendThrowsSynchronously_thenOnlyThatMessageFails() {
    // The value serializer runs inside send(), so a schema-resolution or encryption failure surfaces
    // synchronously. It must fail one message, not abandon the batch.
    Future<RecordMetadata> ok =
        CompletableFuture.completedFuture(
            new RecordMetadata(new TopicPartition(TOPIC, 1), 1, 1, 1, 1, 1));
    doThrow(new SerializationException("boom"))
        .doReturn(ok)
        .when(producer)
        .send(any(ProducerRecord.class));

    SinkerTestKit.TestListIterator iterator = new SinkerTestKit.TestListIterator();
    iterator.addDatum(
        SinkerTestKit.TestDatum.builder().id("1").value("{\"name\":\"Michael\"}".getBytes()).build());
    iterator.addDatum(
        SinkerTestKit.TestDatum.builder().id("2").value("{\"name\":\"Kobe\"}".getBytes()).build());

    ResponseList result = underTest.processMessages(iterator);

    assertEquals(Map.of("1", false, "2", true), successById(result));
  }

  @Test
  void processMessages_whenGlueSchemaResolutionFails_thenMessageIsNotAckedAndErrorIsSurfaced() {
    // With auto-registration off, the Glue serializer throws when the record's schema definition is
    // not registered (or does not byte-exactly match a registered version). That surfaces here as a
    // synchronous AWSSchemaRegistryException from send(). A failure response means Numaflow does NOT
    // ack the message — it is retried — so no record may be reported OK.
    doThrow(
            new com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException(
                "Schema version is not found."))
        .when(producer)
        .send(any(ProducerRecord.class));

    ResponseList result = underTest.processMessages(iterator(Map.of("1", "{\"name\":\"Michael\"}")));

    Response response = result.getResponses().getFirst();
    assertFalse(response.getSuccess(), "a schema-resolution failure must not be acked");
    assertTrue(
        response.getErr().contains("Schema version is not found"),
        "the Glue error must be surfaced so the operator can see why: " + response.getErr());
  }

  @Test
  void processMessages_whenEncryptionFailsForKmsAccessDenied_thenMessageIsNotAcked() {
    // The IAM role lacks kms:GenerateDataKey on the configured key. The EncryptingSerializer runs
    // inside send(), so the KMS denial surfaces synchronously; the message must fail (no ack) and no
    // unencrypted record may be produced as a fallback.
    doThrow(
            software.amazon.awssdk.services.kms.model.KmsException.builder()
                .message(
                    "User is not authorized to perform: kms:GenerateDataKey on the resource")
                .build())
        .when(producer)
        .send(any(ProducerRecord.class));

    ResponseList result = underTest.processMessages(iterator(Map.of("1", "{\"name\":\"Michael\"}")));

    Response response = result.getResponses().getFirst();
    assertFalse(response.getSuccess(), "a KMS access-denied failure must not be acked");
    assertTrue(response.getErr().contains("kms:GenerateDataKey"));
    verify(producer).send(any(ProducerRecord.class)); // attempted once, never retried unencrypted
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
