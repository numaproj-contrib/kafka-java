# Publish envelope-encrypted payloads

### Introduction

This sink can wrap the Kafka message value in an **encryption envelope** before producing it: a data
encryption key (DEK) encrypts the payload with AES-256-GCM, and the DEK itself is wrapped by a
key-management service. A consumer recovers the DEK from that service and decrypts the payload — the
[decrypting source](../../source/envelope-encryption/decrypting-source.md) is the counterpart.

Encryption is **independent of serialization** — it works with any `schemaType`. Whatever the sink
would otherwise have put on the wire is encrypted as-is, so encryption is always the **final** step
before producing.

A **null** or **empty** value is written through unencrypted, whatever the `schemaType`: a null value
has to stay null on the wire to keep its meaning on a compacted topic, and an empty value carries
nothing to protect. The [decrypting source](../../source/envelope-encryption/decrypting-source.md)
mirrors this, handing a null or empty value to its deserializer without decrypting, so the round trip
is symmetric.

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

`ciphertext` is the encryption of whatever the configured `schemaType` produced — for `schemaType:
raw`, the record bytes exactly as they arrived. Base64 is standard with padding, and all fields are in
the message value; Kafka headers are not used.

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

The existing `assumeRoleArn` property, if set, is reused for KMS, so that role must carry
`kms:GenerateDataKey`. One role covers everything this sink talks to, so if your `schemaType` also
uses a schema registry, the same role needs those permissions as well.

Everything else — `schemaType`, Kafka connection, and credentials — is configured exactly as for a
non-encrypted sink.

### DEK rotation

One DEK is generated on first use and reused for the lifetime of the process. Rotation happens by
process restart, so a redeploy rotates it; to rotate without a deployment, restart the producer pod.

Consumers need no coordination for this: every message carries its own `ciphertext_dek`, so a rotation
is transparent.

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
A producer process is assumed not to encrypt more than ~2³² messages under one DEK — the bound NIST
SP 800-38D sets for random 96-bit nonces. With one DEK per process lifetime, that is the total number
of messages the pod produces between restarts; restart the producer to rotate the key well before it
could be approached.

### Failure behavior

Encryption failures fail **that message only**: the sink returns a failure response for it and
continues with the rest of the batch, which is how the sink already treats a payload it cannot
serialize. An unencrypted message is never produced as a fallback.

A malformed key ARN is a **startup** failure: the sink does not start, so it cannot silently produce
plaintext.

Plaintext keys and payloads are never logged. The DEK's plaintext is erased from memory
(best-effort, zero-filled) when the sink shuts down.

### Example

This example generates records, encrypts them, and produces them to a topic. It uses `schemaType:
raw` — bytes in, bytes out — so that encryption is the only thing it demonstrates.

To encrypt Avro instead, add the registry configuration from the [Avro](../avro/avro-sink.md) or
[Glue Avro](../avro-glue/avro-glue-sink.md) sink docs. Nothing about the encryption setup changes.

1. Create the AWS credentials secret (see
   [credentials management](../../credentials-management/protecting-credentials.md)) — its identity
   needs `kms:GenerateDataKey` on the key.

2. Deploy the ConfigMap and pipeline:

   ```bash
   kubectl apply -f manifests/encrypted-producer-config.yaml
   kubectl apply -f manifests/encrypted-producer-pipeline.yaml
   ```

3. Consume the topic with the
   [decrypting source](../../source/envelope-encryption/decrypting-source.md) configured against the
   same key and `schemaType: raw`, and the records come back out identical to what was sunk.
