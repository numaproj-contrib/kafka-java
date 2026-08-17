package io.numaproj.kafka.config;

/**
 * The serialization configuration surface — which schema registry to use, and the Glue
 * serializer/deserializer settings — shared by the source and the sink so a producer and a consumer
 * of one topic are configured with the same key names. The serialization counterpart of {@code
 * EncryptionProps}.
 *
 * <p>{@link #SCHEMA_REGISTRY_TYPE} is consumed by kafka-java itself; the rest are passed through to
 * the Glue serializer/deserializer. The keys under "fixed by kafka-java" below are not part of the
 * user-facing surface: kafka-java sets them and overrides whatever the properties file says, because
 * a different value would break the connector (it exchanges {@code GenericRecord}s) or register a
 * schema on the user's behalf (which kafka-java never does).
 */
final class SerializationProps {

  /** Selects the registry implementation; {@link #TYPE_CONFLUENT} when unset. */
  static final String SCHEMA_REGISTRY_TYPE = "schema.registry.type";

  static final String TYPE_CONFLUENT = "confluent";
  static final String TYPE_GLUE = "glue";

  /** AWS region of the Glue registry; required for {@link #TYPE_GLUE}. */
  static final String REGION = "region";

  /** Glue registry name; {@link #DEFAULT_REGISTRY_NAME} when unset. */
  static final String REGISTRY_NAME = "registry.name";

  static final String DEFAULT_REGISTRY_NAME = "default-registry";

  /**
   * In-frame compression of the Avro body: {@code ZLIB} (frame compression flag {@code 0x05}) or
   * {@code NONE} ({@code 0x00}). User-configurable; kafka-java defaults it to {@link
   * #COMPRESSION_ZLIB}, where the Glue serializer's own default is {@code NONE}.
   */
  static final String COMPRESSION = "compression";

  static final String COMPRESSION_ZLIB = "ZLIB";

  // Fixed by kafka-java; see the class javadoc.

  /** The Glue data format. Avro is the only format the Glue path supports. */
  static final String DATA_FORMAT = "dataFormat";

  static final String DATA_FORMAT_AVRO = "AVRO";

  /**
   * Which Avro reader the Glue <em>deserializer</em> builds. The Glue serializer never reads it — it
   * picks specific or generic from the object it is handed — so this is load-bearing on the source
   * and inert on the sink. The library supplies no default, and {@code UNKNOWN} is rejected at
   * runtime, so the source depends on kafka-java setting it; {@code SPECIFIC_RECORD} would require
   * generated classes the connector does not have.
   */
  static final String AVRO_RECORD_TYPE = "avroRecordType";

  static final String AVRO_RECORD_TYPE_GENERIC = "GENERIC_RECORD";

  /** Glue's schema auto-registration switch. Always off: kafka-java never registers a schema. */
  static final String GLUE_SCHEMA_AUTO_REGISTRATION = "schemaAutoRegistrationEnabled";

  /** How Confluent spells the same thing. Always off, for the same reason. */
  static final String CONFLUENT_AUTO_REGISTER_SCHEMAS = "auto.register.schemas";

  private SerializationProps() {}
}
