# Read envelope-encrypted payloads

### Introduction

Some producers wrap the Kafka message value in an **encryption envelope** before sending it: a data
encryption key (DEK) encrypts the payload with AES-256-GCM, and the DEK itself is wrapped by a
key-management service. This source can transparently **decrypt the value before deserialization**, so
a Numaflow MonoVertex or Pipeline can consume encrypted topics.

Decryption is **independent of serialization** — it composes with any `schemaType` (`avro` with the
Confluent or Glue registry, `json`, or `raw`). The decrypted bytes are handed to the normal
deserializer for that `schemaType`, so the downstream output is identical to the equivalent
non-encrypted topic.

The only key-management backend supported today is **AWS KMS**.

It is **opt-in**: decryption runs only when the AWS KMS key ARN is configured. With the key unset, the
source behaves exactly as before and makes no calls to the key-management service.

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

After decryption, `ciphertext` yields the plaintext the configured `schemaType` expects (for Glue Avro,
a Glue Schema Registry frame; for `raw`, the record bytes; and so on).

### Prerequisites

1. A topic whose values are produced in the envelope format above, with the DEK wrapped by an AWS KMS
   key.

2. AWS credentials available to the pod with permission to decrypt under that key:

   ```json
   {
     "Effect": "Allow",
     "Action": "kms:Decrypt",
     "Resource": "arn:aws:kms:us-east-1:123456789012:key/<key-id>"
   }
   ```

### Configuration

Add the following to `consumer.properties` (managed by kafka-java — consumed internally, not passed to
the Kafka client):

| Property | Required | Default | Description |
|---|---|---|---|
| `payload.envelope.encryption.provider.aws-kms.key.arn` | Yes, to enable decryption | — | Full KMS key ARN. Its presence enables decryption; it is enforced as the `KeyId` on `Decrypt` (KMS rejects ciphertext wrapped under any other key). |
| `payload.envelope.encryption.dek.cache.ttl.ms` | No | `3600000` (1 h) | How long a recovered plaintext DEK is cached in memory to avoid a `Decrypt` call per message. Provider-agnostic (applies regardless of key backend). |

The existing `assumeRoleArn` property (if set) is reused for KMS as well as Glue — a **single assumed
role** covers both, so it must carry `kms:Decrypt` (see IAM above) plus any `glue:*` permissions your
`schemaType` needs. See
[Assuming an IAM role](../avro-glue/avro-glue-source.md#assuming-an-iam-role) for the STS setup.

Everything else — `schemaType`, `schema.registry.type`, Kafka connection, and credentials — is
configured exactly as for a non-encrypted source.

### Example

This example reads from a Glue-Avro topic whose values are envelope-encrypted, and writes to the
built-in log sink.

1. Create the AWS credentials secret (see
   [credentials management](../../credentials-management/protecting-credentials.md)) — the same
   `aws-creds` secret used for Glue works, provided its identity has `kms:Decrypt` on the key.

2. Deploy the ConfigMap and pipeline:

   ```bash
   kubectl apply -f manifests/encrypted-consumer-config.yaml
   kubectl apply -f manifests/encrypted-consumer-pipeline.yaml
   ```

3. Once running, the sink logs show the decrypted, decoded records — identical to the non-encrypted
   Glue-Avro example.

### Failure behavior

The source **fails fast** (logs a clear error and exits, so the pod restarts) on any unrecoverable
condition: a malformed key ARN at startup; or, per message, a value that is not a valid envelope, an
unsupported `alg`, a KMS `Decrypt` failure (including ciphertext wrapped under a different key), or an
authentication-tag failure (tampering / wrong key). A poison or tampered message will therefore
crash-loop the vertex until its offset is advanced or the message is removed. Plaintext keys and
decrypted payloads are never logged.

> **Producer responsibility — nonce uniqueness.** AES-256-GCM is only secure if the producer never
> reuses a nonce under the same DEK. Reuse is catastrophic: it exposes the XOR of the affected
> plaintexts (a two-time pad) and can even let an attacker forge valid authentication tags. The
> consumer **cannot detect or prevent this** — the tag check verifies the integrity of *this*
> message, not that its nonce is unique across all messages under the DEK, and a nonce-reused
> message still decrypts with a valid tag. Guaranteeing per-DEK nonce uniqueness is entirely the
> producer's responsibility.
