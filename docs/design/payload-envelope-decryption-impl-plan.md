# Implementation Plan: Payload Envelope Decryption for the Kafka Source

Companion to [payload-envelope-decryption-spec.md](payload-envelope-decryption-spec.md) (the behavior
source of truth). References like §7.2 / AC4 / RD6 point back to the spec.

## Approach

Insert decryption at the **value-deserializer boundary** (G5). A generic `DecryptingDeserializer<T>`
wraps the delegate the source already uses (Glue Avro, Confluent Avro, or `ByteArray`) and runs
`decrypt → delegate.deserialize`. `ConsumerConfig` builds the delegate and, only when the enable key is
set, wraps it and passes instances to the `KafkaConsumer` constructor. Sourcers, workers, and all
read/ack/offset logic stay **untouched** — the wrapper still yields `GenericRecord` / `byte[]`.

## New package `io.numaproj.kafka.crypto`

| Type | Responsibility | Spec |
|---|---|---|
| `Envelope` (record) | `version, alg, wrappedDek, nonce, ciphertext` | §6.1 |
| `EnvelopeCodec` (iface) + `JsonEnvelopeCodec` | Parse/validate the `json` envelope (`enc_ver==1`, non-blank fields, base64) | §7.1 |
| `KeyProvider` (iface) + `AwsKmsKeyProvider` | `unwrap(wrappedDek)` via KMS `Decrypt` with `KeyId`=ARN; DEK cache | §7.2 |
| `DekCache` | TTL cache keyed by wrapped-DEK bytes; injected `Clock` for testable TTL | §7.2 P2 |
| `PayloadDecryptor` | Orchestrate codec → provider → AEAD (`AES/GCM/NoPadding`, 128-bit tag); reject unsupported `alg` | §6.3 |
| `DecryptingDeserializer<T>` | `delegate.deserialize(topic, decryptor.decrypt(topic, data))`; null/empty passthrough | §5 |
| `EnvelopeDecryptionFactory` | If enable key present → validate ARN (fail-fast) + build decryptor; else `null` | §8, RD6 |

**Security (SR1/SR2/AC6):** never log the DEK, `Envelope`, or decrypted bytes; scrub exception messages.

## AWS wiring (`AwsKmsKeyProvider`)

- Region parsed from the ARN via `software.amazon.awssdk.arns.Arn` (no region key).
- `DecryptRequest.keyId(arn)` so KMS rejects foreign-key ciphertext (AC8).
- Credentials: default chain (IRSA/env); if `assumeRoleArn` set, wrap with `StsAssumeRoleCredentialsProvider` (reuses the Glue key, RD3).

## Wiring `config.ConsumerConfig`

- `decryptor = EnvelopeDecryptionFactory.fromProps(props)` (null when disabled).
- In `kafkaAvroConsumer` / `kafkaByteArrayConsumer`: build the delegate **instance** (Glue / Confluent / `ByteArray`) and `configure(props, false)` it; if `decryptor != null` wrap it; `new KafkaConsumer<>(props, new StringDeserializer(), valueDeserializer)`.
- Kafka does **not** `configure()` constructor-supplied deserializers → we configure the delegate ourselves.
- Strip `payload.envelope.encryption.*` from `props` before building the consumer (as `schema.registry.type` already is). Everything else unchanged.

## Dependency

Add `software.amazon.awssdk:kms:2.32.25` (matches the AWS SDK v2 already pulled by the Glue serde; `sts`/`arns` are transitive).

## Tests (JUnit 5 + Mockito) → AC map

| Test | Covers |
|---|---|
| `JsonEnvelopeCodecTest` | valid parse; `enc_ver!=1`; missing field; bad base64 (AC4) |
| `AwsKmsKeyProviderTest` | unwrap (mock `KmsClient`); `KeyId`=ARN; cache hit ⇒ 1 call; TTL expiry ⇒ new call; access-denied propagates (AC5, AC8) |
| `PayloadDecryptorTest` | round-trip decrypt; unsupported `alg`; tampered ciphertext → `AEADBadTagException` (AC4, AC6) |
| `DecryptingDeserializerTest` | delegate gets decrypted bytes; throw propagates; passthrough (AC1) |
| `ConsumerConfigTest` (extend) | key set ⇒ wrapper around right delegate; absent ⇒ delegate only (AC2); malformed ARN ⇒ throws (AC3) |

Existing `AvroSourcerTest` / `ByteArraySourcerTest` / `AvroWorkerTest` stay green unchanged — the G5 proof. Add a no-leak assertion (AC6) on failure paths.

## Sequencing — one PR, three commits

1. **Commit 1** — crypto package + dependency + its unit tests (no behavior change).
2. **Commit 2** — `ConsumerConfig` wiring + `ConsumerConfigTest`.
3. **Commit 3** — docs (`docs/source/encrypted/…`, generic wording, `kms:Decrypt` IAM scoped to the ARN) + example manifests + README use case.

Ordered so each commit builds and tests green on its own (reviewable commit-by-commit). An e2e variant with a KMS stub/localstack can follow as a separate PR.

## Top risks

- Constructor-supplied deserializers need manual `configure()` (covered in `ConsumerConfigTest`).
- Glue delegate must accept the decrypted GSR frame as if off the wire (verify in Commit 2 / e2e).
- Secret hygiene: audit every log/exception path in `crypto` (SR1/SR2).

**Done when:** every AC has a passing test, `mvn test` is green with existing source tests unchanged, and docs/manifests are merged.
