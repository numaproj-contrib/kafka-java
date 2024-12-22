package io.numaproj.kafka.consumer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

public class Utils {

  // Prepare an avro GenericRecord object for testing, the underlying data is a json string
  static GenericRecord generateTestData() throws IOException {
    String testSchema =
        "{"
            + "\"type\": \"record\","
            + "\"name\": \"User\","
            + "\"fields\": ["
            + "  { \"name\": \"name\", \"type\": \"string\" }"
            + "]"
            + "}";
    var schema = new Schema.Parser().parse(testSchema);
    var record = new GenericData.Record(schema);
    record.put("name", "test-user-name");

    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, outputStream, true);
    writer.write(record, encoder);
    encoder.flush();
    byte[] bytes = outputStream.toByteArray();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, inputStream);
    return reader.read(null, decoder);
  }
}
