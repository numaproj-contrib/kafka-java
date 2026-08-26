# Read from several topics with one source

### Introduction

One source vertex can consume several Kafka topics on the same cluster, merged into a single stream
and sharing one consumer group. Each message carries the topic it came from in the
`X-NF-Kafka-TopicName` header, so a downstream vertex can tell them apart.

This replaces running one MonoVertex or Pipeline per topic when several low-throughput topics all
feed the same downstream processing.

Set `topicNames` instead of `topicName`. The two are mutually exclusive, and exactly one is
required — a deployment that sets neither, or both, fails at startup.

### When to use it

Use one source for several topics when all of these hold:

* The topics are on the **same Kafka cluster** — there is one `bootstrap.servers`.
* They share the **same `schemaType`**, and for Avro the same deserializer configuration.
* They have **similar throughput**.
* They have **similar partition counts**.
* One **shared consumer group** and one shared `onError` policy suit all of them.

### When not to use it

| Situation | Why, and what to do instead |
|---|---|
| Throughput differs by an order of magnitude | One batch is filled from whichever topics have records, so a busy topic crowds out a quiet one. Use separate source vertices feeding the same downstream vertex over the ISB (fan-in). |
| Topics on different clusters | Not supported; the source has one `bootstrap.servers`. Use one source per cluster. |
| Mixed formats | The source picks one `schemaType` for the whole process. A mismatched topic fails per message at runtime, with nothing detectable at startup. |
| A per-topic failure policy is needed | `onError` applies to the whole source. Split the topics across sources. |
| A per-topic consumer group is needed | All the topics share one group. Split the topics across sources. |

### Configuration

| Key | Required | Description |
|---|---|---|
| `topicNames` | Yes, unless `topicName` is set | The Kafka topics to read, as a YAML list or a comma-separated string. Entries are trimmed; a repeat is dropped with a warning; a blank entry is rejected at startup. |
| `topicName` | Yes, unless `topicNames` is set | A single topic. Existing single-topic deployments keep using this and are unaffected by this feature. |
| `schemaType` | Yes | `avro`, `json` or `raw`. Applies to every topic. |
| `onError` | No | `fail` (default) or `skip`. Applies to every topic. See [`onError`](../on-error.md). |

`topicNames` accepts either spelling:

```yaml
  user.configuration: |
    topicNames:
      - orders
      - payments
    schemaType: raw
```

```yaml
  user.configuration: |
    topicNames: orders,payments
    schemaType: raw
```

Multi-topic is **source-only**. A sink deployment that sets `topicNames` is rejected at startup;
use `topicName`.

### Example

#### Pre-requisite

Create the topics `orders` and `payments` in your Kafka cluster and produce some messages to each.

#### Configure the Kafka consumer

Use the example [ConfigMap](manifests/multi-topic-consumer-config.yaml) to configure the sourcer,
then deploy it to the cluster.

#### Create the pipeline

Use the example [pipeline](manifests/multi-topic-consumer-pipeline.yaml), which reads both topics
into the Numaflow builtin log sink. Make sure the args list under the source vertex matches the file
paths in the ConfigMap.

#### Observe the log sink

Messages from both topics arrive on the one sink, interleaved. The builtin log sink prints the
message headers, so the originating topic is visible directly:

```
Payload - {"orderId":"a406ad8d","total":42} Keys -  EventTime - 1736439076729 Headers - X-NF-Kafka-TopicName: orders,  ID - ...
Payload - {"paymentId":"6792d656","amount":42} Keys -  EventTime - 1736439076731 Headers - X-NF-Kafka-TopicName: payments,  ID - ...
```

To act on the topic rather than just read it in a log, take the `X-NF-Kafka-TopicName` header in a
user-defined vertex or sink — see [message headers](../message-headers.md).

### Reading the topic name downstream

Every message carries the topic it was read from:

```java
String topic = datum.getHeaders().get("X-NF-Kafka-TopicName");
```

The source sets this **after** copying the record's own Kafka headers, so a producer that wrote a
header of the same name cannot disguise the real topic. See
[message headers](../message-headers.md).

### Watermarks and partition IDs

Numaflow tracks one watermark per partition ID, so each Kafka partition across all the topics needs
a distinct ID. Topics are sorted by name and each is given a contiguous range of IDs the size of the
largest topic's partition count. With `orders` (8 partitions) and `payments` (4):

| Topic | Partition IDs |
|---|---|
| `orders` | 0–7 |
| `payments` | 8–11 (12–15 held in reserve) |

Two consequences worth knowing:

* **A single topic keeps its bare Kafka partition numbers**, so an existing deployment that stays on
  `topicName` sees no change and keeps its watermark state.
* **Scaling a topic's partition count requires restarting the source pods.** The map is built once at
  startup. If a topic grows past the reserved range, the source exits and the restarted pod
  recomputes the map against the current counts. (Numaflow's builtin Rust Kafka source instead
  re-fetches metadata on every call; this implementation trades that for a restart.)

A topic that does not exist yet is rejected at startup under `topicNames`, because an understated
partition count would overlap another topic's range once the topic appeared. Single-topic mode still
tolerates it.

### Pending count and idle topics

`getPending` reports the summed lag across every configured topic, so a MonoVertex scales on the
total backlog.

The source watermark is the minimum across active partitions, so a rarely-active topic holds back
the combined watermark for everything. If that matters, configure Numaflow's
[idle source](https://numaflow.numaproj.io/user-guide/reference/idle-source/) handling.

### Throughput fairness

There is no per-topic rate limiting or prioritization: one consumer subscribes to all the topics and
`max.poll.records` is shared across them, so a batch skews toward whichever topics have records.
Kafka's fetcher rotates across partitions between polls, so no topic starves permanently, but it is
not fair. This matches Numaflow's builtin Kafka source. It is why the guidance above is to use this
mode only for topics of similar throughput.

### Recovering from a bad record

Under `onError: fail` a record the source cannot read crashes the vertex, and the pod crash-loops
until the record is dealt with. To get past it:

1. Set `onError: skip` in `user.configuration` and let the pods restart.
2. Find the dropped record in the source pod logs. Each drop logs at **`WARN`**:
   ```
   Dropping bad record topic:orders partition:2 offset:9481
   ```
   The line names the topic, partition and offset only — never the record's contents.
3. Watch `kafka_java_source_skipped_messages_total`, which carries a `topic` label, to see which
   topic the drops are coming from and when they stop.
4. Fix the producer and republish the affected messages.
5. Set `onError` back to `fail`.

Republishing is only safe when the downstream pipeline is order-insensitive: the corrected message
lands at a later offset than the one it replaces.

### Choose MonoVertex

Although a Pipeline is used to demonstrate, it is highly recommended to use the
[MonoVertex](https://numaflow.numaproj.io/core-concepts/monovertex/) to build your streaming data
processing application on Numaflow. The way you specify the source specification stays the same.

### Protect your credentials

In the example, the `consumer.properties` contains the credentials. Please see
[credentials management](../../credentials-management/protecting-credentials.md) to protect your
credentials.
