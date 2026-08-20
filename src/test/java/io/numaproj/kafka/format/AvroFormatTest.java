package io.numaproj.kafka.format;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class AvroFormatTest {

  private static final Schema SCHEMA =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}");

  @Test
  void roundTrip_recordToJsonAndBack() throws Exception {
    GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("name", "alice");

    byte[] json = AvroFormat.forSink(SCHEMA).toPayload(record);
    assertEquals("{\"name\":\"alice\"}", new String(json));

    GenericRecord parsed = AvroFormat.forSink(SCHEMA).toRecord(json);
    assertEquals("alice", parsed.get("name").toString());
  }

  @Test
  void toRecord_invalidJson_throwsFormatException() {
    FormatException e =
        assertThrows(
            FormatException.class, () -> AvroFormat.forSink(SCHEMA).toRecord("{\"age\":1}".getBytes()));
    assertTrue(e.getMessage().contains("Failed to prepare avro generic record"));
  }

  @Test
  void toPayload_typeMismatch_throwsFormatExceptionNotRawRuntimeException() {
    // A malformed record surfaces as a RuntimeException (AvroTypeException), not an IOException,
    // and must still be reported as a FormatException so that onError governs it.
    GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("name", 12345); // schema declares "name" as a string

    FormatException e =
        assertThrows(FormatException.class, () -> AvroFormat.forSource().toPayload(record));
    // The cause is stripped to class name + stack trace: AvroTypeException's message embeds the
    // offending datum, which for the encrypted path would leak decrypted field values into logs.
    assertNotNull(e.getCause());
    assertTrue(e.getCause().getMessage().endsWith("Exception"), "cause message must be a class name");
    assertFalse(e.getCause().getMessage().contains("12345"));
    assertTrue(e.getCause().getStackTrace().length > 0);
  }

  @Test
  void forSink_nullSchema_rejected() {
    assertThrows(IllegalArgumentException.class, () -> AvroFormat.forSink(null));
  }

  @Test
  void sourceFormat_cannotSerialize() {
    assertThrows(FormatException.class, () -> AvroFormat.forSource().toRecord("{}".getBytes()));
  }
}
