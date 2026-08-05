# Publish envelope-encrypted payloads

### Introduction

This sink can wrap the Kafka message value in an **encryption envelope** before producing it: a data
encryption key (DEK) encrypts the payload with AES-256-GCM, and the DEK itself is wrapped by a
key-management service. A consumer recovers the DEK from that service and decrypts the payload — the
[decrypting source](../../source/envelope-encryption/decrypting-source.md) is the counterpart.

Encryption is **independent of serialization** — it composes with any `schemaType` (`avro` with the
Confluent or Glue registry, `json`, or `raw`). Whatever the value serializer produced is encrypted
as-is, so encryption is always the **final** step before producing.

The only key-management backend supported today is **AWS KMS**.

It is **opt-in**: encryption runs only when the AWS KMS key ARN is configured. With the key unset, the
sink behaves exactly as before and makes no calls to the key-management service.

### Envelope format

The Kafka message value is a JSON object:

```json
{
  "enc_ver": 1,
  "alg": "AES-256-GCM",
  "ciphertext_dek": "<base64 KMS-wrapped DEK>",
  "nonce": "<base64 12-byte nonce>",
  "ciphertext": "<base64 AES-256-GCM output, 16-byte tag appended>"
}
```

`ciphertext` is the encryption of whatever the configured `schemaType` produced (for Glue Avro, a Glue
Schema Registry frame; for `raw`, the record bytes; and so on). Base64 is standard with padding, and
all fields are in the message value — Kafka headers are not used.

### Prerequisites

1. A KMS key the sink can generate data keys under, and AWS credentials available to the pod:

   ```json
   {
     "Effect": "Allow",
     "Action": "kms:GenerateDataKey",
     "Resource": "arn:aws:kms:us-east-1:123456789012:key/<key-id>"
   }
   ```

2. Whoever consumes the topic needs `kms:Decrypt` on the same key.

### Configuration

Add the following to `producer.properties` (managed by kafka-java — consumed internally, not passed to
the Kafka client):

| Property | Required | Default | Description |
|---|---|---|---|
| `payload.envelope.encryption.provider.aws-kms.key.arn` | Yes, to enable encryption | — | Full KMS key ARN. Its presence enables encryption, and the region is derived from it. A bare alias is **not** accepted — resolve it to a key ARN first. |
| `payload.envelope.encryption.dek.ttl.ms` | No | `3600000` (1 h) | How long one generated DEK is reused before a fresh `GenerateDataKey` call. See *DEK rotation* below. |

The existing `assumeRoleArn` property (if set) is reused for KMS as well as Glue — a **single assumed
role** covers both, so it must carry `kms:GenerateDataKey` plus any `glue:*` permissions your
`schemaType` needs.

Everything else — `schemaType`, `schema.registry.type`, Kafka connection, and credentials — is
configured exactly as for a non-encrypted sink.

### DEK rotation

One DEK is generated on first use and reused until its TTL elapses, then a fresh one is generated. The
DEK is also new on every process restart, so a redeploy rotates it.

Consumers need no coordination for this: every message carries its own `ciphertext_dek`, so a rotation
is transparent. Bounding the reuse window also bounds how many messages share a single key.

### Nonce uniqueness — what this sink guarantees

AES-256-GCM is only secure if a nonce is never reused under the same DEK. Reuse is catastrophic: it
exposes the XOR of the affected plaintexts (a two-time pad) and can even let an attacker forge valid
authentication tags. A consumer **cannot detect this** — a nonce-reused message still decrypts with a
valid tag — so the obligation rests entirely on the producer, which here is this sink.

How it is met:

* A fresh 12-byte (96-bit) nonce is drawn for **every message** from a single long-lived
  `SecureRandom`, including when the DEK is reused across messages.
* Nonces are never derived from message content, and never from a counter that could restart at zero
  after a crash or a rescale.
* The DEK reuse window (above) bounds how many messages are encrypted under one key, which bounds the
  collision probability inherent to random nonces.

If you lower `payload.envelope.encryption.dek.ttl.ms`, you get more frequent rotation and therefore
fewer messages per key; raising it does the opposite. The default of one hour is a deliberate middle
ground.

### Failure behavior

Encryption failures fail **that message only**: the sink returns a failure response for it and
continues with the rest of the batch, which is how the sink already treats a payload it cannot
serialize. An unencrypted message is never produced as a fallback.

A malformed key ARN is a **startup** failure: the sink does not start, so it cannot silently produce
plaintext.

Plaintext keys and payloads are never logged.

### Example

This example generates records, frames them as Glue Avro, encrypts them, and produces them to a topic.

1. Create the AWS credentials secret (see
   [credentials management](../../credentials-management/protecting-credentials.md)) — the same
   `aws-creds` secret used for Glue works, provided its identity has `kms:GenerateDataKey` on the key.

2. Deploy the ConfigMap and pipeline:

   ```bash
   kubectl apply -f manifests/encrypted-producer-config.yaml
   kubectl apply -f manifests/encrypted-producer-pipeline.yaml
   ```

3. Consume the topic with the
   [decrypting source](../../source/envelope-encryption/decrypting-source.md) configured against the
   same key, and the records come back out identical to what was sunk.
