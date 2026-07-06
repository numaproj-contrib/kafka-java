package io.numaproj.kafka.encryption;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Base64;

/**
 * The {@code json} envelope codec. The Kafka value is a JSON object:
 *
 * <pre>
 * {
 *   "enc_ver": 1,
 *   "alg": "AES-256-GCM",
 *   "ciphertext_dek": "&lt;base64 wrapped DEK&gt;",
 *   "nonce": "&lt;base64 12-byte nonce&gt;",
 *   "ciphertext": "&lt;base64 AEAD output, 16-byte tag appended&gt;"
 * }
 * </pre>
 *
 * <p>The version lives in {@code enc_ver} (the only supported value is {@code 1}); it is not encoded
 * in the codec name (spec §7.1).
 */
public class JsonEnvelopeCodec implements EnvelopeCodec {

  static final int SUPPORTED_ENC_VER = 1;

  private static final String F_ENC_VER = "enc_ver";
  private static final String F_ALG = "alg";
  private static final String F_CIPHERTEXT_DEK = "ciphertext_dek";
  private static final String F_NONCE = "nonce";
  private static final String F_CIPHERTEXT = "ciphertext";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public Envelope parse(String topic, byte[] value) {
    JsonNode node;
    try {
      node = MAPPER.readTree(value);
    } catch (Exception e) {
      throw new PayloadDecryptionException("Value is not a JSON envelope", e);
    }
    if (node == null || !node.isObject()) {
      throw new PayloadDecryptionException("Value is not a JSON envelope object");
    }
    int encVer = requireInt(node, F_ENC_VER);
    if (encVer != SUPPORTED_ENC_VER) {
      throw new PayloadDecryptionException("Unsupported enc_ver: " + encVer);
    }
    String alg = requireText(node, F_ALG);
    byte[] wrappedDek = requireBase64(node, F_CIPHERTEXT_DEK);
    byte[] nonce = requireBase64(node, F_NONCE);
    byte[] ciphertext = requireBase64(node, F_CIPHERTEXT);
    return new Envelope(encVer, alg, wrappedDek, nonce, ciphertext);
  }

  private static int requireInt(JsonNode node, String field) {
    JsonNode n = node.get(field);
    if (n == null || n.isNull() || !n.isIntegralNumber()) {
      throw new PayloadDecryptionException("Missing or non-integer field: " + field);
    }
    return n.intValue();
  }

  private static String requireText(JsonNode node, String field) {
    JsonNode n = node.get(field);
    if (n == null || n.isNull() || !n.isTextual() || n.asText().isBlank()) {
      throw new PayloadDecryptionException("Missing or blank field: " + field);
    }
    return n.asText();
  }

  private static byte[] requireBase64(JsonNode node, String field) {
    String text = requireText(node, field);
    try {
      return Base64.getDecoder().decode(text);
    } catch (IllegalArgumentException e) {
      throw new PayloadDecryptionException("Field is not valid base64: " + field, e);
    }
  }
}
