package io.numaproj.kafka.consumer;

import static org.junit.jupiter.api.Assertions.*;

import io.numaproj.kafka.common.BadRecordException;
import io.numaproj.kafka.encryption.PayloadDecryptionException;
import io.numaproj.kafka.format.FormatException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkException;

class ReadErrorReasonTest {

  @Test
  void of_payloadDecryptionException_isBadData() {
    assertEquals(
        ReadErrorReason.BAD_DATA,
        ReadErrorReason.of(new PayloadDecryptionException("bad envelope")));
  }

  @Test
  void of_formatException_isBadData() {
    assertEquals(ReadErrorReason.BAD_DATA, ReadErrorReason.of(new FormatException("bad avro")));
  }

  @Test
  void of_badRecordExceptionNestedBehindDeserializationWrapper_isBadData() {
    RecordDeserializationException wrapper =
        new RecordDeserializationException(
            RecordDeserializationException.DeserializationExceptionOrigin.VALUE,
            new TopicPartition("t", 0),
            1L,
            0L,
            TimestampType.CREATE_TIME,
            ByteBuffer.allocate(0),
            ByteBuffer.allocate(0),
            new RecordHeaders(),
            "boom",
            new BadRecordException("malformed"));

    // Classification reads the cause chain, not the RecordDeserializationException wrapper.
    assertEquals(ReadErrorReason.BAD_DATA, ReadErrorReason.of(wrapper.getCause()));
  }

  @Test
  void of_bareRuntimeException_isUnknown() {
    assertEquals(ReadErrorReason.UNKNOWN, ReadErrorReason.of(new RuntimeException("boom")));
  }

  @Test
  void of_ioException_isUnknown() {
    assertEquals(ReadErrorReason.UNKNOWN, ReadErrorReason.of(new IOException("boom")));
  }

  @Test
  void of_untranslatedSdkException_isUnknown() {
    assertEquals(
        ReadErrorReason.UNKNOWN, ReadErrorReason.of(SdkException.create("slow down", null)));
  }
}
