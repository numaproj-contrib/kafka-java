package io.numaproj.kafka.crypto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Test;

class JsonEnvelopeCodecTest {

  private final JsonEnvelopeCodec codec = new JsonEnvelopeCodec();

  private static String b64(byte[] b) {
    return Base64.getEncoder().encodeToString(b);
  }

  private static byte[] bytes(String json) {
    return json.getBytes(StandardCharsets.UTF_8);
  }

  private static String envelope(int encVer, String alg, byte[] dek, byte[] nonce, byte[] ct) {
    return String.format(
        "{\"enc_ver\":%d,\"alg\":\"%s\",\"ciphertext_dek\":\"%s\",\"nonce\":\"%s\",\"ciphertext\":\"%s\"}",
        encVer, alg, b64(dek), b64(nonce), b64(ct));
  }

  @Test
  void parsesValidEnvelope() {
    byte[] dek = {1, 2, 3};
    byte[] nonce = new byte[12];
    byte[] ct = {9, 8, 7, 6};
    Envelope e = codec.parse("t", bytes(envelope(1, "AES-256-GCM", dek, nonce, ct)));

    assertEquals(1, e.version());
    assertEquals("AES-256-GCM", e.alg());
    assertArrayEquals(dek, e.wrappedDek());
    assertArrayEquals(nonce, e.nonce());
    assertArrayEquals(ct, e.ciphertext());
  }

  @Test
  void rejectsUnsupportedEncVer() {
    byte[] any = {1};
    assertThrows(
        PayloadDecryptionException.class,
        () -> codec.parse("t", bytes(envelope(2, "AES-256-GCM", any, any, any))));
  }

  @Test
  void rejectsMissingField() {
    // No ciphertext_dek.
    String json = "{\"enc_ver\":1,\"alg\":\"AES-256-GCM\",\"nonce\":\"AAAA\",\"ciphertext\":\"AAAA\"}";
    assertThrows(PayloadDecryptionException.class, () -> codec.parse("t", bytes(json)));
  }

  @Test
  void rejectsBlankAlg() {
    byte[] any = {1};
    assertThrows(
        PayloadDecryptionException.class,
        () -> codec.parse("t", bytes(envelope(1, "", any, any, any))));
  }

  @Test
  void rejectsBadBase64() {
    String json =
        "{\"enc_ver\":1,\"alg\":\"AES-256-GCM\",\"ciphertext_dek\":\"AAAA\",\"nonce\":\"not*base64!\",\"ciphertext\":\"AAAA\"}";
    assertThrows(PayloadDecryptionException.class, () -> codec.parse("t", bytes(json)));
  }

  @Test
  void rejectsNonJson() {
    assertThrows(
        PayloadDecryptionException.class,
        () -> codec.parse("t", "not json".getBytes(StandardCharsets.UTF_8)));
  }
}
