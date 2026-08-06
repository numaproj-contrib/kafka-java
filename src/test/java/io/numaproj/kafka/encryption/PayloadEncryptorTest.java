package io.numaproj.kafka.encryption;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PayloadEncryptorTest {

  private static final byte[] WRAPPED_DEK = {7, 7, 7, 7};
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static byte[] dek() {
    byte[] b = new byte[32];
    new SecureRandom().nextBytes(b);
    return b;
  }

  private static PayloadEncryptor encryptorWith(byte[] plaintextDek) {
    return new PayloadEncryptor(
        new JsonEnvelopeCodec(), new FixedDekSource(plaintextDek, WRAPPED_DEK));
  }

  /**
   * The interop proof: what the sink writes must be readable by the shipped source-side decryptor,
   * using the same JSON codec and a DEK unwrapper that returns the same key KMS would have.
   */
  @Test
  void encryptedPayloadIsReadableByTheSourceDecryptor() {
    byte[] plaintextDek = dek();
    byte[] payload = "a GSR frame would go here".getBytes(StandardCharsets.UTF_8);

    byte[] wireValue = encryptorWith(plaintextDek).encrypt(payload);

    DekUnwrapper unwrapper = mock(DekUnwrapper.class);
    when(unwrapper.unwrap(any())).thenReturn(plaintextDek);
    PayloadDecryptor decryptor = new PayloadDecryptor(new JsonEnvelopeCodec(), unwrapper);

    assertArrayEquals(payload, decryptor.decrypt(wireValue));
  }

  @Test
  void writesTheContractEnvelopeFields() throws Exception {
    byte[] wireValue = encryptorWith(dek()).encrypt("payload".getBytes(StandardCharsets.UTF_8));

    JsonNode json = MAPPER.readTree(wireValue);
    assertEquals(1, json.get("enc_ver").asInt());
    assertEquals("AES-256-GCM", json.get("alg").asText());
    assertArrayEquals(
        WRAPPED_DEK, Base64.getDecoder().decode(json.get("ciphertext_dek").asText()));
    // 12-byte nonce, and the ciphertext carries the 16-byte tag appended to the 7-byte payload.
    assertEquals(12, Base64.getDecoder().decode(json.get("nonce").asText()).length);
    assertEquals(7 + 16, Base64.getDecoder().decode(json.get("ciphertext").asText()).length);
  }

  @Test
  void usesStandardPaddedBase64NotUrlSafe() throws Exception {
    // A 4-byte wrapped DEK encodes to 8 base64 chars with padding; the URL encoder would omit '='
    // and use '-'/'_' for 62/63.
    byte[] wireValue = encryptorWith(dek()).encrypt("payload".getBytes(StandardCharsets.UTF_8));

    String dekField = MAPPER.readTree(wireValue).get("ciphertext_dek").asText();
    assertEquals(Base64.getEncoder().encodeToString(WRAPPED_DEK), dekField);
    assertTrue(dekField.endsWith("="), "standard base64 keeps padding");
  }

  /**
   * AES-GCM security rests on never reusing a nonce under one key, and the sink reuses a DEK across
   * messages by design — so every message must draw a fresh nonce.
   */
  @Test
  void drawsAFreshNoncePerMessageUnderTheSameDek() throws Exception {
    PayloadEncryptor encryptor = encryptorWith(dek());
    Set<String> nonces = new HashSet<>();
    Set<String> ciphertexts = new HashSet<>();

    for (int i = 0; i < 200; i++) {
      JsonNode json = MAPPER.readTree(encryptor.encrypt("same payload".getBytes(StandardCharsets.UTF_8)));
      nonces.add(json.get("nonce").asText());
      ciphertexts.add(json.get("ciphertext").asText());
    }

    assertEquals(200, nonces.size(), "every message must use a distinct nonce");
    // Identical plaintext under one DEK must still produce distinct ciphertext.
    assertEquals(200, ciphertexts.size());
  }

  @Test
  void twoEncryptionsOfTheSamePayloadDiffer() {
    PayloadEncryptor encryptor = encryptorWith(dek());
    byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);

    assertNotEquals(
        new String(encryptor.encrypt(payload), StandardCharsets.UTF_8),
        new String(encryptor.encrypt(payload), StandardCharsets.UTF_8));
  }
}
