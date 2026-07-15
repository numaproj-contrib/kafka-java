package io.numaproj.kafka.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

/**
 * Avro format.
 *
 * <p>Source direction: an Avro {@link GenericRecord} is rendered to its JSON representation so it
 * can be forwarded to the next Numaflow vertex as bytes. The record's own schema is used, so no
 * fixed schema is required — use {@link #forSource()}.
 *
 * <p>Sink direction: an incoming JSON payload is decoded into a {@link GenericRecord} using a fixed
 * schema (typically fetched from the schema registry); the Kafka client then serializes it with the
 * Avro serializer. Use {@link #forSink(Schema)}.
 */
@Slf4j
public class AvroFormat implements KafkaFormat<GenericRecord> {

  private final Schema schema;

  private AvroFormat(Schema schema) {
    this.schema = schema;
  }

  /** Creates a source-side format; the schema is taken from each record. */
  public static AvroFormat forSource() {
    return new AvroFormat(null);
  }

  /** Creates a sink-side format that decodes incoming JSON payloads against the given schema. */
  public static AvroFormat forSink(Schema schema) {
    if (schema == null) {
      throw new IllegalArgumentException("Avro schema must not be null for a sink");
    }
    return new AvroFormat(schema);
  }

  @Override
  public byte[] toPayload(GenericRecord value) throws FormatException {
    Schema recordSchema = value.getSchema();
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(recordSchema);
      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(recordSchema, out);
      writer.write(value, encoder);
      encoder.flush();
      return out.toByteArray();
    } catch (IOException e) {
      throw new FormatException("Failed to convert the Avro record to JSON format", e);
    }
  }

  @Override
  public GenericRecord toRecord(byte[] payload) throws FormatException {
    // Assumes the input payload is JSON matching the configured schema.
    if (schema == null) {
      throw new FormatException("Avro source format cannot serialize records to Kafka");
    }
    try {
      DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
      Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new String(payload));
      return reader.read(null, decoder);
    } catch (Exception e) {
      throw new FormatException("Failed to prepare avro generic record", e);
    }
  }
}
