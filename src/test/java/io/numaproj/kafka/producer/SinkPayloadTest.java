package io.numaproj.kafka.producer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.numaproj.kafka.config.UserConfig;
import io.numaproj.kafka.encryption.Dek;
import io.numaproj.kafka.encryption.DekGenerator;
import io.numaproj.kafka.encryption.EncryptingSerializer;
import io.numaproj.kafka.encryption.JsonEnvelopeCodec;
import io.numaproj.kafka.encryption.PayloadEncryptor;
import io.numaproj.kafka.format.AvroFormat;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.SinkerTestKit;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.zip.InflaterInputStream;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * End-to-end check of the value the sink puts on the wire, against the published data contract:
 * serialize to a Glue frame, then encrypt, then produce.
 *
 * <p>Runs the real {@link KafkaSinker} and the real {@link GlueSchemaRegistryKafkaSerializer}. No AWS
 * calls are made: KMS is a mocked {@link DekGenerator}, and the Glue serializer is given a fixed
 * schema-version id, which is the one case where it does not consult the registry (otherwise it
 * resolves the schema definition, which is what happens in production).
 */
class SinkPayloadTest {

  private static final String TOPIC = "numagen-avro";
  private static final String SCHEMA_JSON =
      """
      {"type":"record","name":"numagen","fields":[\
      {"name":"Data","type":{"type":"record","name":"Data","fields":[{"name":"value","type":"long"}]}},\
      {"name":"Createdts","type":"long"}]}""";

  // Bytes 2..18 of the frame. The value from the serialization contract's worked example.
  private static final UUID SCHEMA_VERSION_ID = UUID.fromString("7d3d848b-fa70-4d12-aee9-118ab5075c92");

  private static final byte[] PLAINTEXT_DEK = new byte[32];
  private static final byte[] WRAPPED_DEK = "wrapped-dek-from-kms".getBytes(StandardCharsets.UTF_8);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    new SecureRandom().nextBytes(PLAINTEXT_DEK);
  }

  /** The real Glue serializer, pinned to a fixed schema-version id so it makes no registry call. */
  private static Serializer<GenericRecord> glueSerializer() {
    Map<String, Object> configs = new HashMap<>();
    configs.put("region", "us-east-1");
    configs.put("dataFormat", "AVRO");
    configs.put("compression", "ZLIB");
    configs.put("avroRecordType", "GENERIC_RECORD");
    configs.put("schemaAutoRegistrationEnabled", "false");

    GlueSchemaRegistryKafkaSerializer serializer =
        new GlueSchemaRegistryKafkaSerializer(configs, SCHEMA_VERSION_ID);
    serializer.configure(configs, false);
    @SuppressWarnings("unchecked")
    Serializer<GenericRecord> typed = (Serializer<GenericRecord>) (Serializer<?>) serializer;
    return typed;
  }

  private static PayloadEncryptor encryptor() {
    DekGenerator generator = mock(DekGenerator.class);
    when(generator.generate()).thenReturn(new Dek(PLAINTEXT_DEK, WRAPPED_DEK));
    return new PayloadEncryptor(new JsonEnvelopeCodec(), generator);
  }

  @Test
  void sinkProducesAnEncryptedGlueFramedAvroPayload() throws Exception {
    Schema schema = new Schema.Parser().parse(SCHEMA_JSON);

    // 1. The generic record the pipeline is sinking, and the JSON payload form the sink accepts.
    GenericRecord data = new GenericData.Record(schema.getField("Data").schema());
    data.put("value", 1736439076729944818L);
    GenericRecord record = new GenericData.Record(schema);
    record.put("Data", data);
    record.put("Createdts", 1736439076729944818L);
    String jsonPayload = "{\"Data\":{\"value\":1736439076729944818},\"Createdts\":1736439076729944818}";

    // 2. A sink whose value serializer is encrypt(glueFrame(record)).
    UserConfig userConfig = mock(UserConfig.class);
    when(userConfig.getTopicName()).thenReturn(TOPIC);
    @SuppressWarnings("unchecked")
    KafkaProducer<String, GenericRecord> producer = mock(KafkaProducer.class);
    Serializer<GenericRecord> valueSerializer =
        new EncryptingSerializer<>(glueSerializer(), encryptor());

    ArgumentCaptor<ProducerRecord<String, GenericRecord>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    Future<RecordMetadata> ok =
        CompletableFuture.completedFuture(
            new RecordMetadata(new TopicPartition(TOPIC, 0), 0, 0, 0, 0, 0));
    doReturn(ok).when(producer).send(any(ProducerRecord.class));

    KafkaSinker<GenericRecord> sinker =
        new KafkaSinker<>(userConfig, producer, AvroFormat.forSink(schema));

    SinkerTestKit.TestListIterator iterator = new SinkerTestKit.TestListIterator();
    iterator.addDatum(
        SinkerTestKit.TestDatum.builder().id("1").value(jsonPayload.getBytes(StandardCharsets.UTF_8)).build());

    // 3. Run the sink.
    ResponseList responses = sinker.processMessages(iterator);
    assertTrue(responses.getResponses().getFirst().getSuccess(), "sink must accept the payload");

    // A mocked KafkaProducer does not run serializers, so take the record the sinker handed it and
    // serialize that — the same value serializer ProducerConfig installs on the real producer.
    // ProducerConfigTest covers the wiring; this covers what the wiring produces.
    org.mockito.Mockito.verify(producer).send(captor.capture());
    assertEquals(TOPIC, captor.getValue().topic());
    byte[] wireValue = valueSerializer.serialize(TOPIC, captor.getValue().value());

    // 4a. The value is the JSON encryption envelope.
    JsonNode envelope = MAPPER.readTree(wireValue);
    assertEquals(1, envelope.get("enc_ver").asInt());
    assertTrue(envelope.get("enc_ver").isInt(), "enc_ver is an integer");
    assertEquals("AES-256-GCM", envelope.get("alg").asText());
    String dekField = envelope.get("ciphertext_dek").asText();
    assertArrayEquals(WRAPPED_DEK, Base64.getDecoder().decode(dekField));
    assertEquals(Base64.getEncoder().encodeToString(WRAPPED_DEK), dekField, "padded standard base64");

    byte[] nonce = Base64.getDecoder().decode(envelope.get("nonce").asText());
    byte[] ciphertext = Base64.getDecoder().decode(envelope.get("ciphertext").asText());
    assertEquals(12, nonce.length, "12-byte nonce");

    // 4b. It decrypts under the DEK KMS handed out, and the tag verifies.
    byte[] frame = decrypt(PLAINTEXT_DEK, nonce, ciphertext);

    // 4c. The plaintext is a Glue Schema Registry frame.
    assertEquals(0x03, frame[0] & 0xFF, "header version byte");
    assertEquals(0x05, frame[1] & 0xFF, "compression flag must be zlib");
    ByteBuffer uuidBytes = ByteBuffer.wrap(frame, 2, 16);
    assertEquals(
        SCHEMA_VERSION_ID,
        new UUID(uuidBytes.getLong(), uuidBytes.getLong()),
        "schema-version uuid at bytes 2..18");

    // 4d. The body is RFC 1950 zlib (not raw DEFLATE) and Avro-decodes to the original record.
    assertEquals(0x78, frame[18] & 0xFF, "RFC 1950 zlib header byte");
    byte[] avroBody = inflate(frame, 18);
    GenericRecord decoded =
        new GenericDatumReader<GenericRecord>(schema)
            .read(null, DecoderFactory.get().binaryDecoder(avroBody, null));
    assertEquals(record, decoded, "round-trips back to the original record");

    // 5. Leave the payload behind so it can be inspected by hand.
    Path out = Path.of("target", "sink-payload.json");
    Files.createDirectories(out.getParent());
    Files.write(out, wireValue);
    System.out.println("wrote " + out.toAbsolutePath());
    System.out.println("frame head: " + hex(frame, 24));

    valueSerializer.close();
  }

  private static byte[] decrypt(byte[] dek, byte[] nonce, byte[] ciphertext) throws Exception {
    Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
    cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(128, nonce));
    return cipher.doFinal(ciphertext);
  }

  private static byte[] inflate(byte[] frame, int offset) throws Exception {
    byte[] body = new byte[frame.length - offset];
    System.arraycopy(frame, offset, body, 0, body.length);
    try (InflaterInputStream in = new InflaterInputStream(new ByteArrayInputStream(body));
        ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      in.transferTo(out);
      return out.toByteArray();
    }
  }

  private static String hex(byte[] bytes, int limit) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < Math.min(bytes.length, limit); i++) {
      sb.append(String.format("%02x ", bytes[i]));
    }
    return sb.toString().trim();
  }
}
