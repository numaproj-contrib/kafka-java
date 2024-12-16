package io.numaproj.confluent.kafka_sink.sinker;

import io.numaproj.confluent.kafka_sink.config.UserConfig;
import io.numaproj.confluent.kafka_sink.schema.Registry;
import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.SinkerTestKit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class KafkaSinkerTest {

    private final UserConfig userConfig = mock(UserConfig.class);
    private final KafkaProducer producer = mock(KafkaProducer.class);
    private final Registry schemaRegistry = mock(Registry.class);

    private static final String TEST_TOPIC = "test-topic";

    private KafkaSinker underTest;

    @BeforeEach
    public void setUp() {
        String USER_SCHEMA_JSON = "{"
                + "\"type\": \"record\","
                + "\"name\": \"User\","
                + "\"fields\": ["
                + "  { \"name\": \"name\", \"type\": \"string\" }"
                + "]"
                + "}";
        var schema = new Schema.Parser().parse(USER_SCHEMA_JSON);
        // MockitoAnnotations.initMocks(this);
        when(userConfig.getTopicName()).thenReturn(TEST_TOPIC);
        when(schemaRegistry.getAvroSchema(TEST_TOPIC)).thenReturn(schema);
        underTest = new KafkaSinker(userConfig, producer, schemaRegistry);
    }

    @Test
    @SuppressWarnings("unchecked")
    void processMessages_responseSuccess() {
        SinkerTestKit.TestDatum testDatum1 = SinkerTestKit.TestDatum.builder()
                .id("1")
                .value("{\"name\": \"Michael Jordan\"}".getBytes())
                .build();
        SinkerTestKit.TestDatum testDatum2 = SinkerTestKit.TestDatum.builder()
                .id("2")
                .value("{\"name\": \"Kobe Bryant\"}".getBytes())
                .build();
        SinkerTestKit.TestListIterator datumIterator = new SinkerTestKit.TestListIterator();
        datumIterator.addDatum(testDatum1);
        datumIterator.addDatum(testDatum2);
        Future<RecordMetadata> recordMetadataFuture = CompletableFuture.completedFuture(new RecordMetadata(
                new TopicPartition(userConfig.getTopicName(), 1), 1, 1, 1, 1, 1));
        doReturn(recordMetadataFuture).when(producer).send(any(ProducerRecord.class));
        ResponseList responseList = underTest.processMessages(datumIterator);
        List<Response> responses = responseList.getResponses();
        Response response1 = Response.responseOK("1");
        Response response2 = Response.responseOK("2");
        Map<String, Response> wantResponseMap = new HashMap<>();
        wantResponseMap.put(response1.getId(), response1);
        wantResponseMap.put(response2.getId(), response2);
        assertEquals(wantResponseMap.size(), responses.size(), "response objects are equal");
        // no direct way to compare the Response object at the moment so check individually
        for (Response gotResponse : responses) {
            Response wantResponse = wantResponseMap.get(gotResponse.getId());
            assertEquals(wantResponse.getSuccess(), gotResponse.getSuccess());
            assertEquals(wantResponse.getId(), gotResponse.getId());
            wantResponseMap.remove(gotResponse.getId());
        }
        assertTrue(wantResponseMap.isEmpty(), "expected all the response object match as expected");
    }

    @Test
    @SuppressWarnings("unchecked")
    void processMessages_responseFailure_schemaNotFound() {
        SinkerTestKit.TestDatum testDatum1 = SinkerTestKit.TestDatum.builder()
                .id("1")
                .value("{\"name\": \"Michael Jordan\"}".getBytes())
                .build();
        SinkerTestKit.TestDatum testDatum2 = SinkerTestKit.TestDatum.builder()
                .id("2")
                .value("{\"name\": \"Kobe Bryant\"}".getBytes())
                .build();
        SinkerTestKit.TestListIterator datumIterator = new SinkerTestKit.TestListIterator();
        datumIterator.addDatum(testDatum1);
        datumIterator.addDatum(testDatum2);
        Future<RecordMetadata> recordMetadataFuture = CompletableFuture.completedFuture(new RecordMetadata(
                new TopicPartition(userConfig.getTopicName(), 1), 1, 1, 1, 1, 1));

        // mock schema not found
        when(schemaRegistry.getAvroSchema(TEST_TOPIC)).thenReturn(null);

        doReturn(recordMetadataFuture).when(producer).send(any(ProducerRecord.class));
        ResponseList responseList = underTest.processMessages(datumIterator);
        List<Response> responses = responseList.getResponses();
        Response response1 = Response.responseFailure("1", "Failed to retrieve schema for topic");
        Response response2 = Response.responseFailure("2", "Failed to retrieve schema for topic");
        Map<String, Response> wantResponseMap = new HashMap<>();
        wantResponseMap.put(response1.getId(), response1);
        wantResponseMap.put(response2.getId(), response2);
        assertEquals(wantResponseMap.size(), responses.size(), "response objects are equal");
        // no direct way to compare the Response object at the moment so check individually
        for (Response gotResponse : responses) {
            Response wantResponse = wantResponseMap.get(gotResponse.getId());
            assertEquals(wantResponse.getSuccess(), gotResponse.getSuccess());
            if (wantResponse.getErr() != null) {
                assertTrue(gotResponse.getErr().contains(wantResponse.getErr()));
            }

            assertEquals(wantResponse.getId(), gotResponse.getId());
            wantResponseMap.remove(gotResponse.getId());
        }
        assertTrue(wantResponseMap.isEmpty(), "expected all the response object match as expected");
    }

    @Test
    @SuppressWarnings("unchecked")
    void processMessages_responseFailure_schemaNotMatchData() {
        SinkerTestKit.TestDatum testDatum1 = SinkerTestKit.TestDatum.builder()
                .id("1")
                .value("{\"age\": \"60\"}".getBytes())
                .build();
        SinkerTestKit.TestDatum testDatum2 = SinkerTestKit.TestDatum.builder()
                .id("2")
                .value("{\"age\": \"41\"}".getBytes())
                .build();
        SinkerTestKit.TestListIterator datumIterator = new SinkerTestKit.TestListIterator();
        datumIterator.addDatum(testDatum1);
        datumIterator.addDatum(testDatum2);
        Future<RecordMetadata> recordMetadataFuture = CompletableFuture.completedFuture(new RecordMetadata(
                new TopicPartition(userConfig.getTopicName(), 1), 1, 1, 1, 1, 1));

        doReturn(recordMetadataFuture).when(producer).send(any(ProducerRecord.class));
        ResponseList responseList = underTest.processMessages(datumIterator);
        List<Response> responses = responseList.getResponses();
        Response response1 = Response.responseFailure("1", "Failed to prepare avro generic record");
        Response response2 = Response.responseFailure("2", "Failed to prepare avro generic record");
        Map<String, Response> wantResponseMap = new HashMap<>();
        wantResponseMap.put(response1.getId(), response1);
        wantResponseMap.put(response2.getId(), response2);
        assertEquals(wantResponseMap.size(), responses.size(), "response objects are equal");
        // no direct way to compare the Response object at the moment so check individually
        for (Response gotResponse : responses) {
            Response wantResponse = wantResponseMap.get(gotResponse.getId());
            assertEquals(wantResponse.getSuccess(), gotResponse.getSuccess());
            if (wantResponse.getErr() != null) {
                assertTrue(gotResponse.getErr().contains(wantResponse.getErr()));
            }
            assertEquals(wantResponse.getId(), gotResponse.getId());
            wantResponseMap.remove(gotResponse.getId());
        }
        assertTrue(wantResponseMap.isEmpty(), "expected all the response object match as expected");
    }

    @Test
    @SuppressWarnings("unchecked")
    void processMessages_responseFailure_futureFails() {
        SinkerTestKit.TestDatum testDatum1 = SinkerTestKit.TestDatum.builder()
                .id("1")
                .value("{\"name\": \"Michael Jordan\"}".getBytes())
                .build();
        SinkerTestKit.TestDatum testDatum2 = SinkerTestKit.TestDatum.builder()
                .id("2")
                .value("{\"name\": \"Kobe Bryant\"}".getBytes())
                .build();
        SinkerTestKit.TestListIterator datumIterator = new SinkerTestKit.TestListIterator();
        datumIterator.addDatum(testDatum1);
        datumIterator.addDatum(testDatum2);
        Future<RecordMetadata> recordMetadataFuture = CompletableFuture.completedFuture(new RecordMetadata(
                new TopicPartition(userConfig.getTopicName(), 1), 1, 1, 1, 1, 1));
        doAnswer(e -> {
            ProducerRecord<String, GenericRecord> pr = e.getArgument(0);
            GenericRecord value = pr.value();
            if (value.get("name").toString().equals("Michael Jordan")) {
                return CompletableFuture.failedFuture(new Exception("future error"));
            }
            return recordMetadataFuture;
        }).when(producer).send(any(ProducerRecord.class));

        // doReturn(recordMetadataFuture).when(producer).send(any(ProducerRecord.class));
        ResponseList responseList = underTest.processMessages(datumIterator);
        List<Response> responses = responseList.getResponses();
        Response response1 = Response.responseFailure("1", "future error");
        Response response2 = Response.responseOK("2");
        Map<String, Response> wantResponseMap = new HashMap<>();
        wantResponseMap.put(response1.getId(), response1);
        wantResponseMap.put(response2.getId(), response2);
        assertEquals(wantResponseMap.size(), responses.size(), "response objects are equal");
        // no direct way to compare the Response object at the moment so check individually
        for (Response gotResponse : responses) {
            Response wantResponse = wantResponseMap.get(gotResponse.getId());
            assertEquals(wantResponse.getSuccess(), gotResponse.getSuccess());
            if (wantResponse.getErr() != null) {
                assertTrue(gotResponse.getErr().contains(wantResponse.getErr()));
            }
            assertEquals(wantResponse.getId(), gotResponse.getId());
            wantResponseMap.remove(gotResponse.getId());
        }
        assertTrue(wantResponseMap.isEmpty(), "expected all the response object match as expected");
    }

    @Test
    @SuppressWarnings("unchecked")
    void destroy_inflightMessagesProcessed() throws InterruptedException {
        SinkerTestKit.TestDatum testDatum1 = SinkerTestKit.TestDatum.builder()
                .id("1")
                .value("{\"name\": \"Michael Jordan\"}".getBytes())
                .build();
        SinkerTestKit.TestDatum testDatum2 = SinkerTestKit.TestDatum.builder()
                .id("2")
                .value("{\"name\": \"Kobe Bryant\"}".getBytes())
                .build();
        SinkerTestKit.TestListIterator datumIterator = new SinkerTestKit.TestListIterator();
        datumIterator.addDatum(testDatum1);
        datumIterator.addDatum(testDatum2);

        Future<RecordMetadata> recordMetadataFuture = CompletableFuture.completedFuture(new RecordMetadata(
                new TopicPartition(userConfig.getTopicName(), 1), 1, 1, 1, 1L, 1, 1));

        doAnswer(e -> {
                    // intentionally slow down the process
                    Thread.sleep(1000);
                    return recordMetadataFuture;
                }
        ).when(producer).send(any(ProducerRecord.class));
        final ResponseList[] responseList = {null};

        CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            responseList[0] = underTest.processMessages(datumIterator);
            countDownLatch.countDown();
        });
        thread.start();
        underTest.destroy();
        countDownLatch.await();

        List<Response> responses = responseList[0].getResponses();
        Response response1 = Response.responseOK("1");
        Response response2 = Response.responseOK("2");
        Map<String, Response> wantResponseMap = new HashMap<>();
        wantResponseMap.put(response1.getId(), response1);
        wantResponseMap.put(response2.getId(), response2);
        assertEquals(wantResponseMap.size(), responses.size(), "response objects are equal");
        // no direct way to compare the Response object at the moment so check individually
        for (Response gotResponse : responses) {
            Response wantResponse = wantResponseMap.get(gotResponse.getId());
            assertEquals(wantResponse.getSuccess(), gotResponse.getSuccess());
            assertEquals(wantResponse.getId(), gotResponse.getId());
            wantResponseMap.remove(gotResponse.getId());
        }
        assertTrue(wantResponseMap.isEmpty(), "expected all the response object match as expected");
    }
}
