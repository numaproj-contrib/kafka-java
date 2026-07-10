package io.numaproj.kafka.encryption;

/**
 * The parsed encryption envelope: the pieces a codec extracts from a Kafka value so the payload can
 * be decrypted. The wire layout that produced this is owned by the {@link EnvelopeCodec}.
 *
 * @param version the envelope format version (e.g. {@code enc_ver})
 * @param alg the AEAD algorithm name (e.g. {@code AES-256-GCM})
 * @param wrappedDek the wrapped data encryption key (unwrapped by the key-management backend)
 * @param nonce the AEAD nonce / IV
 * @param ciphertext the AEAD ciphertext (authentication tag appended)
 */
public record Envelope(int version, String alg, byte[] wrappedDek, byte[] nonce, byte[] ciphertext) {}
