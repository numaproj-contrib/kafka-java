# Specification: Payload Envelope Decryption for the Kafka Source

## 1. Background

A Kafka message value carries two independent concerns:

1. **Serialization** — how the record bytes are shaped (Confluent Avro, Glue Avro, JSON, or raw).
   `kafka-java` already supports these via its `schemaType` / `schema.registry.type` configuration.
2. **Encryption** (optional) — whether the value is wrapped in an encryption envelope before being
   produced to Kafka.

These layers are orthogonal: encryption produces opaque ciphertext bytes; what those bytes contain
once decrypted (a Glue frame, a Confluent Avro payload, JSON, or raw bytes) is a separate concern.

Envelope encryption is a standard pattern: a data encryption key (DEK) encrypts the payload with a
symmetric AEAD cipher, and the DEK itself is wrapped by a key-management service. The consumer
recovers the DEK from that service, then decrypts the payload. (The generic term "key-management
service" is used throughout; the term "AWS KMS" refers specifically to the `aws-kms` implementation in
§7.2.)

Today `kafka-java` has no decryption step: the raw Kafka value is passed straight to the configured
value deserializer. It therefore cannot consume topics whose payloads are envelope-encrypted.

## 2. Problem statement

When a topic's payloads are envelope-encrypted, the Kafka message value is ciphertext, not a
deserializable payload. The source must **decrypt the value before deserialization**, for any
serialization path, while leaving non-encrypted topics behavior unchanged.

The **key-management backend** (how the wrapped DEK is unwrapped) and the **envelope wire format** (how
the encrypted parts are laid out on the wire) both vary between organizations, so each is kept as a
distinct internal seam behind an interface. This first pass ships one of each — the `aws-kms` backend
and the `json` wire format — and leaves the door open for others (e.g. HashiCorp Vault or GCP KMS
backends; a binary wire format) without further design changes.

Neither seam is exposed as a configuration *selector* in this pass (see §12, RD5, RD6): only one of
each is implemented, so a selector would be speculative. Decryption is enabled simply by configuring the
shipped backend (its key); a `provider` / `format` selector can be added later as a backward-compatible
option if a second implementation is ever needed.

## 3. Goals

- **G1.** Consume envelope-encrypted topics: decrypt the value, then deserialize the plaintext via the
  existing path, producing the same downstream output as an equivalent non-encrypted topic.
- **G2.** Decryption is **opt-in** and does not change behavior when disabled.
- **G3.** Decryption is applied uniformly for **all** serialization paths (`avro` with Confluent or
  Glue registry, `json`, `raw`) — honoring the "encryption ⟂ serialization" principle.
- **G4.** The key-management backend (§6.2) and the envelope wire format (§6.1) are kept as separate
  internal seams behind interfaces. Adding a new key provider or a new wire format must not require
  changing the other, the source, or the read path. Neither is exposed as a configuration selector in
  this pass (see §12, RD5, RD6); each can gain one later as a backward-compatible option.
- **G5.** Keep the change localized: do not alter the source's read/ack/offset logic. Concretely,
  `AvroSourcer`/`ByteArraySourcer` `read()`/`ack()`/`getPending()`/`getPartitions()`, the
  `readTopicPartitionOffsetMap` read↔ack cross-check, and the worker thread/queue/poll/commit
  machinery remain untouched. Decryption is inserted at the deserialization boundary as a wrapper
  around the value deserializer, matching the existing "config selects the deserializer" pattern.

## 4. Non-goals

- **N1.** Producer/sink encryption (writing encrypted payloads). Source only.
- **N2.** Dead-letter queue routing. On any unrecoverable message the source **fails fast** (see §8).
- **N3.** Additional codecs or key providers beyond `json` and `aws-kms`. The extension-point
  interfaces (Service Provider Interfaces, or SPIs) must accommodate them, but only these two are
  implemented in this pass.
- **N4.** Transport security (TLS). Independent of payload encryption and already the user's Kafka config.
- **N5.** Key-management provisioning. Keys are provisioned out-of-band by the operator.
- **N6.** Auto-detecting the envelope format from message content. The wire format is fixed (`json`)
  in this pass, not sniffed; in-format evolution is handled by the `enc_ver` version field inside the
  envelope.

## 5. Design overview

```
Kafka value (bytes)
   │
   ▼
[ DecryptingDeserializer<T> ]   ← inserted only when decryption is enabled
   │   parsed    = EnvelopeCodec.parse(topic, value)     // {version, alg, wrappedDek, nonce, ciphertext}
   │   dek       = KeyProvider.unwrap(parsed.wrappedDek) // plaintext DEK (cached)
   │   plaintext = aead_decrypt(parsed.alg, dek, parsed.nonce, parsed.ciphertext)
   │   return delegate.deserialize(topic, plaintext)
   ▼
delegate value deserializer (Glue Avro | Confluent Avro | ByteArray)
   ▼
existing read/ack/offset logic (unchanged)
```

- **`EnvelopeCodec`** (§6.1) — parses the wire format into structured fields. An internal seam, fixed
  to `json` (not configurable in this pass).
- **`KeyProvider`** (§6.2) — unwraps the wrapped DEK into a plaintext DEK. An internal seam, fixed to
  `aws-kms` (enabled by the presence of its config, not a configuration selector).
- **AEAD decrypt** (§6.3) — generic given `(alg, dek, nonce, ciphertext)`; driven by the `alg` the
  codec reports.
- **`DecryptingDeserializer<T>`** — a generic Kafka `Deserializer` that composes the above with the
  delegate deserializer the source would otherwise use. Because it delegates, it works for every
  serialization path (`T` = `GenericRecord` for Avro, `byte[]` for JSON/raw).
- When decryption is disabled, no wrapper is inserted and behavior is unchanged.

## 6. Extension points

### 6.1 `EnvelopeCodec` (wire format)

- `configure(config)` — read codec-specific properties.
- `Envelope parse(String topic, byte[] value)` — parse into `{version, alg, wrappedDek, nonce,
  ciphertext}`, validating structure/version; throw on any unrecoverable error (§8).

Requirements:
- **EC1.** A codec owns the wire layout only (field names/positions, encoding, value-vs-header
  placement, its own version field). It performs **no** key-unwrap calls and **no** decryption.
- **EC2.** The codec is an internal seam, fixed to `json` in this pass; it is not selected by
  configuration. If a second format is added later, a `payload.envelope.encryption.format` selector and
  a per-codec `payload.envelope.encryption.format.<name>.*` namespace can be introduced as a
  backward-compatible option (see §12, RD5).

### 6.2 `KeyProvider` (key-management backend)

- `configure(config)` — read provider-specific properties; establish clients/credentials.
- `byte[] unwrap(byte[] wrappedDek)` — return the plaintext DEK; throw on any unrecoverable error (§8).
- `close()` — release resources.

Requirements:
- **KP1.** A provider owns key unwrapping and DEK caching only. It is independent of the wire format.
- **KP2.** A provider has its own configuration namespace
  `payload.envelope.encryption.provider.<name>.*`.

### 6.3 Core orchestration

- **CO1.** The core composes `codec → provider → AEAD`, contains no format- or provider-specific
  logic, and depends only on the two interfaces (not on any specific implementation).
- **CO2.** The AEAD step is selected by the `alg` reported by the codec. The only supported algorithm
  in this pass is `AES-256-GCM` (`AES/GCM/NoPadding`, 128-bit tag). An unsupported `alg` is an
  unrecoverable error.

## 7. Built-in implementations

Only the `aws-kms` key provider is implemented at this time; the `KeyProvider` seam (§6.2) admits
others (e.g. HashiCorp Vault, GCP KMS) without further design changes.

### 7.1 Codec: `json`

The Kafka message value is a JSON object:

```json
{
  "enc_ver": 1,
  "alg": "AES-256-GCM",
  "ciphertext_dek": "<base64 wrapped DEK>",
  "nonce": "<base64 12-byte nonce>",
  "ciphertext": "<base64 AEAD output, 16-byte tag appended>"
}
```

- Base64 is standard (with padding). `ciphertext` is the AEAD output with the 16-byte authentication
  tag appended.
- `enc_ver` versions this envelope format; the only currently supported value is `1`.
- `alg` is surfaced to the core AEAD step (§6.3).
- All fields are in the message **value**; Kafka headers are not used.
- Validation: require `enc_ver == 1` (integer) and a non-blank `alg`, `ciphertext_dek`, `nonce`,
  `ciphertext`. A structural mismatch is an unrecoverable error.

### 7.2 Key provider: `aws-kms`

- **P1. Unwrap.** Recover the plaintext DEK via AWS KMS `Decrypt` on the wrapped DEK, **passing the
  configured key ARN as `KeyId`** so KMS rejects any ciphertext not wrapped under the expected key.
- **P2. DEK cache.** Cache recovered plaintext DEKs **keyed by the wrapped-DEK bytes** (not by
  topic), with a configurable TTL. On cache hit, do not call AWS KMS. (The wrapped DEK changes on
  producer restart and two producers may briefly use different DEKs on one topic.)
- **P3. Region.** The KMS client region is derived from the configured key ARN.
- **P4. Credentials.** Follow the same model as the Glue integration (IRSA / Pod Identity preferred;
  env-var credentials and `assumeRoleArn` supported).

## 8. Error handling

The source **fails fast** on any unrecoverable error, logging a clear message and terminating the
process (consistent with the source's existing `kill` → `System.exit(100)` behavior), causing the pod
to restart.

Startup:
- The configured key ARN is malformed (for `aws-kms`: not a well-formed KMS key ARN
  `arn:aws:kms:<region>:<account>:key/<id>`). An *absent* key ARN is not an error — it simply leaves
  decryption disabled.

Per message (when enabled):
- Value not parseable by the configured codec, or codec validation failure (for `json`: §7.1).
- Unsupported `alg` (CO2).
- Key unwrap failure (for `aws-kms`: access-denied, invalid-ciphertext, or wrapped under a key other
  than the configured `KeyId`).
- AEAD authentication failure (tampering / wrong key), i.e. `AEADBadTagException`.

> **Accepted consequence:** a single poison or tampered message will crash-loop the vertex until the
> offset is advanced or the message is removed. This is the deliberate trade-off of fail-fast over
> dead-lettering, and matches how the source already treats decode failures.

## 9. Configuration surface (consumer.properties)

New keys, managed by `kafka-java` (consumed internally, not forwarded verbatim to `KafkaConsumer`):

| Key | Required | Default | Description |
|---|---|---|---|
| `payload.envelope.encryption.provider.aws-kms.key.arn` | No | — | **Presence enables decryption.** Full KMS key ARN, enforced as `KeyId` on `Decrypt`; region derived from it. Bare aliases are not accepted. |
| `payload.envelope.encryption.provider.aws-kms.dek.cache.ttl.ms` | No | `3600000` (1 h) | Plaintext-DEK cache TTL. |

Reused existing key: `assumeRoleArn` (if set, assume that role for AWS KMS as well as Glue).

Enablement rule: decryption applies for any `schemaType` when
`payload.envelope.encryption.provider.aws-kms.key.arn` is set (non-blank). A blank/whitespace value is
treated as unset (disabled). The key backend (`aws-kms`) and wire format (`json`) are fixed and not
selectable in this pass.

## 10. Security requirements

- **SR1.** The plaintext DEK must never be logged, persisted, or placed in error messages, metrics,
  or traces.
- **SR2.** Decrypted plaintext payload must never be logged on failure.
- **SR3.** For `aws-kms`, the required `kms:Decrypt` IAM permission should be scoped to the configured
  key ARN (`Resource: <key-arn>`), not `"*"`.

## 11. Acceptance criteria

- **AC1.** For each serialization path (`avro`+Glue, `avro`+Confluent, `json`, `raw`), a pipeline
  reading an envelope-encrypted topic produces the same downstream records as the equivalent
  non-encrypted topic.
- **AC2.** With decryption disabled (no key ARN configured), existing behavior across all paths is
  byte-for-byte unchanged, with no key-management calls.
- **AC3 (startup fail-fast).** The source fails to start (reads no messages) when the configured
  `aws-kms` key ARN is malformed (not a well-formed KMS key ARN). An absent key ARN is not an error — it
  leaves decryption disabled (AC2).
- **AC4 (per-message fail-fast).** A malformed or untrusted message triggers fail-fast with no partial
  output, including when: codec validation fails (for `json`: `enc_ver != 1` or a missing field);
  `alg` is unsupported; the wrapped DEK was produced under a key other than the configured one (KMS
  rejects it); or the ciphertext is tampered (AEAD authentication failure).
- **AC5.** For `aws-kms`: two consecutive messages sharing the same wrapped DEK result in exactly one
  KMS `Decrypt` call; a message after TTL expiry triggers a new call.
- **AC6.** No log line, metric, or error contains a plaintext DEK or decrypted payload.
- **AC7.** The key provider is swappable without touching the envelope-parsing or decrypt logic
  (demonstrated by the wiring and, if practical, a test-double provider).
- **AC8.** Unit tests cover §6–§8; docs and example manifests describe the new use case generically.

## 12. Resolved decisions

- **RD1.** The `EnvelopeCodec` / `KeyProvider` SPIs are **internal seams**, not public
  extension points. Only this repo implements them for now, so their signatures may be refactored
  freely; they are not a stability commitment to third parties and need no interface versioning. They
  may be promoted to a documented public API later once the shape is proven.
- **RD2.** The DEK-cache TTL is **not** bounded by the connector; it is left to operator
  discretion (default `3600000` ms / 1 h).
- **RD3.** No provider-specific `assumeRoleArn`. `aws-kms` **reuses the Glue `assumeRoleArn`**
  when set. A dedicated key may be added later only if a real deployment needs KMS and Glue to assume
  different roles.
- **RD4.** **Log-only** on decrypt failure for this pass (no new metrics). The source has no
  metrics story yet (only `// TODO - emit error metrics` markers); emitting decrypt-failure metrics is
  deferred to a broader observability effort.
- **RD5.** The envelope wire format is **not** exposed as a configuration axis in this pass; it is fixed
  to `json` as an internal seam. Shipping exactly one codec makes a `format` selector speculative
  (YAGNI), and RD1 already lets us refactor the seam freely. Introducing the selector later is
  backward-compatible provided it defaults to `json`. The codec remains a distinct internal component
  (not fused into the key provider) so that future addition is a localized change.
- **RD6.** The key backend is likewise **not** exposed as a configuration selector in this pass; the
  single shipped backend (`aws-kms`) is enabled by the presence of its own config
  (`...aws-kms.key.arn`). Shipping exactly one backend makes a `provider` selector speculative (YAGNI),
  symmetric with RD5. The `provider.aws-kms.*` namespace is retained so a top-level
  `payload.envelope.encryption.provider` selector can be added later, **defaulting to `aws-kms`** — a
  backward-compatible change. `KeyProvider` remains a distinct internal seam (RD1) so that addition is
  localized.
