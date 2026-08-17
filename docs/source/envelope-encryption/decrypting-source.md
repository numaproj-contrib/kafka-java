# Read envelope-encrypted payloads

### Introduction

Some producers wrap the Kafka message value in an **encryption envelope** before sending it: a data
encryption key (DEK) encrypts the payload with AES-256-GCM, and the DEK itself is wrapped by a
key-management service. This source can transparently **decrypt the value before deserialization**, so
a Numaflow MonoVertex or Pipeline can consume encrypted topics.

Decryption is **independent of serialization** — it works with any `schemaType`. The decrypted bytes
are handed to the normal deserializer for that `schemaType`, so the downstream output is identical to
the equivalent non-encrypted topic.

A **null** or **empty** value is handed to the deserializer without decrypting, mirroring the
[encrypting sink](../../sink/envelope-encryption/encrypting-sink.md), which writes both through
unencrypted — the round trip is symmetric.

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

After decryption, `ciphertext` yields the plaintext the configured `schemaType` expects — for
`schemaType: raw`, the record bytes, forwarded as they are.

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

The existing `assumeRoleArn` property, if set, is reused for KMS, so that role must carry
`kms:Decrypt` (see IAM above). One role covers everything this source talks to, so if your
`schemaType` also uses a schema registry, the same role needs those permissions as well. See
[Assuming an IAM role](../avro-glue/avro-glue-source.md#assuming-an-iam-role) for the STS setup.

Everything else — `schemaType`, Kafka connection, and credentials — is configured exactly as for a
non-encrypted source.

### Example

This example reads a topic whose values are envelope-encrypted and writes to the built-in log sink.
It uses `schemaType: raw` — bytes in, bytes out — so that decryption is the only thing it
demonstrates.

To decrypt Avro instead, add the registry configuration from the [Avro](../avro/avro-source.md) or
[Glue Avro](../avro-glue/avro-glue-source.md) source docs. Nothing about the decryption setup changes.

1. Create the AWS credentials secret (see
   [credentials management](../../credentials-management/protecting-credentials.md)) — its identity
   needs `kms:Decrypt` on the key.

2. Deploy the ConfigMap and pipeline:

   ```bash
   kubectl apply -f manifests/encrypted-consumer-config.yaml
   kubectl apply -f manifests/encrypted-consumer-pipeline.yaml
   ```

3. Once running, the log sink shows the decrypted values — identical to what the
   [encrypting sink](../../sink/envelope-encryption/encrypting-sink.md) produced.

### Failure behavior

By default, the source **fails fast** (logs a clear error and exits, so the pod restarts) on any
unrecoverable condition: a malformed key ARN at startup; or, per message, a value that is not a valid
envelope, an unsupported `alg`, a KMS `Decrypt` failure (including ciphertext wrapped under a different
key), or an authentication-tag failure (tampering / wrong key). A poison or tampered message will
therefore crash-loop the vertex until its offset is advanced or the message is removed. Plaintext keys
and decrypted payloads are never logged.

#### `onError: skip`

Setting `onError: skip` in `user.configuration` (source-only - the sink ignores it) changes this: a
record that fails to be read is dropped and counted instead of crashing the vertex. A `WARN` log
identifies the dropped record by `topic`/`partition`/`offset` only - never the record itself - and
`kafka_java_source_read_errors_total{stage="decode", ..., action="skipped"}` is incremented. See
[source metrics](../../metrics/source-metrics.md) for the full metric and an alerting query.

The record is **lost** - there is no dead-letter queue yet (`BadRecordSink` is the seam a future one
will use).

Today, `skip` drops **every** decode failure, including one **not** attributable to the record's own
bytes - for example a KMS throttle, an expired credential, or a Glue/Confluent schema-registry outage.
This is deliberate for now: reliably distinguishing "this record's bytes are corrupt" from "the
environment is unavailable" cannot be done cheaply across AWS SDK and schema-registry exception
taxonomies (see the design notes in the source code). The failure is still classified as a metric
label - `reason="bad_data"` vs. `reason="unknown"` - so the risk is measurable even though it is not
yet gated:

```promql
increase(kafka_java_source_read_errors_total{reason="unknown"}[5m]) > 0
```

An alert firing on this query means records were dropped for a reason not attributable to their own
bytes - most likely an incident (KMS or schema-registry outage, expired credentials) discarding good
records rather than bad ones. A follow-up will retry or circuit-break such failures instead of skipping
them.

**Startup failures always fail fast**, regardless of `onError`: a malformed key ARN, for instance, is
a configuration error, not a per-record one, and `onError` never applies to it.

**Kafka authentication failures are not per-record either.** A `SaslAuthenticationException` (e.g. from
MSK IAM auth) fails the consumer as a whole, not a single record, and correctly kills the vertex under
any `onError` setting rather than being skipped.

> **Producer responsibility — nonce uniqueness.** AES-256-GCM is only secure if the producer never
> reuses a nonce under the same DEK. Reuse is catastrophic: it exposes the XOR of the affected
> plaintexts (a two-time pad) and can even let an attacker forge valid authentication tags. The
> consumer **cannot detect or prevent this** — the tag check verifies the integrity of *this*
> message, not that its nonce is unique across all messages under the DEK, and a nonce-reused
> message still decrypts with a valid tag. Guaranteeing per-DEK nonce uniqueness is entirely the
> producer's responsibility.
