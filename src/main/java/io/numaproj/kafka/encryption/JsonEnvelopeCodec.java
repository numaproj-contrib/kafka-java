package io.numaproj.kafka.encryption;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
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
 * in the codec name.
 */
public class JsonEnvelopeCodec implements EnvelopeCodec {

  static final int SUPPORTED_ENC_VER = 1;

  // Ignore unknown properties so a producer adding envelope fields later does not break decoding.
  private static final ObjectMapper MAPPER =
      new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  /** Structural binding of the envelope. Base64 fields stay strings and are decoded explicitly. */
  private record EnvelopeJson(
      @JsonProperty("enc_ver") Integer encVer,
      @JsonProperty("alg") String alg,
      @JsonProperty("ciphertext_dek") String ciphertextDek,
      @JsonProperty("nonce") String nonce,
      @JsonProperty("ciphertext") String ciphertext) {}

  @Override
  public Envelope parse(byte[] value) {
    EnvelopeJson e;
    try {
      e = MAPPER.readValue(value, EnvelopeJson.class);
    } catch (Exception ex) {
      throw new PayloadDecryptionException("Value is not a JSON envelope", ex);
    }
    if (e == null) {
      throw new PayloadDecryptionException("Value is not a JSON envelope object");
    }
    if (e.encVer() == null || e.encVer() != SUPPORTED_ENC_VER) {
      throw new PayloadDecryptionException("Unsupported enc_ver: " + e.encVer());
    }
    return new Envelope(
        e.encVer(),
        requireText("alg", e.alg()),
        requireBase64("ciphertext_dek", e.ciphertextDek()),
        requireBase64("nonce", e.nonce()),
        requireBase64("ciphertext", e.ciphertext()));
  }

  private static String requireText(String field, String value) {
    if (value == null || value.isBlank()) {
      throw new PayloadDecryptionException("Missing or blank field: " + field);
    }
    return value;
  }

  private static byte[] requireBase64(String field, String value) {
    String text = requireText(field, value);
    try {
      return Base64.getDecoder().decode(text);
    } catch (IllegalArgumentException e) {
      throw new PayloadDecryptionException("Field is not valid base64: " + field, e);
    }
  }
}
