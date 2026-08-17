package io.numaproj.kafka.encryption;

/**
 * A data encryption key in both the forms a producer needs: the plaintext key it encrypts with, and
 * the wrapped form it puts on the wire so a consumer can recover the plaintext from the
 * key-management backend.
 *
 * <p>The plaintext must never be logged, persisted, or placed in an error message.
 *
 * @param plaintext the 256-bit AES key, in memory only
 * @param wrapped the same key encrypted by the key-management backend, safe to publish
 */
public record Dek(byte[] plaintext, byte[] wrapped) {}
