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
  void forSink_nullSchema_rejected() {
    assertThrows(IllegalArgumentException.class, () -> AvroFormat.forSink(null));
  }

  @Test
  void sourceFormat_cannotSerialize() {
    assertThrows(FormatException.class, () -> AvroFormat.forSource().toRecord("{}".getBytes()));
  }
}
