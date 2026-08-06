package io.numaproj.kafka.schema;

import io.numaproj.kafka.common.aws.AwsCredentials;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaVersionNumber;

/**
 * {@link Registry} backed by the AWS Glue Schema Registry.
 *
 * <p>Glue addresses a schema by registry name + schema name + version number, so the {@code subject}
 * argument is the Glue <em>schema name</em> and the registry name comes from configuration.
 *
 * <p>The sink needs the schema <em>definition</em> in order to decode an incoming JSON payload into a
 * {@code GenericRecord}. The Glue serializer then independently resolves that same definition back to
 * a schema-version id when it frames the record, which is why schema auto-registration must stay off:
 * a definition that does not already exist in the registry fails rather than silently registering.
 *
 * <p>Owns the Glue client and its {@link AwsCredentials}; {@link #close()} releases both.
 */
@Slf4j
public class GlueRegistry implements Registry {

  private final GlueClient glue;
  private final AwsCredentials credentials;
  private final String registryName;

  GlueRegistry(GlueClient glue, AwsCredentials credentials, String registryName) {
    this.glue = glue;
    this.credentials = credentials;
    this.registryName = registryName;
  }

  /**
   * Builds the owned client stack (Glue client + credentials). If the Glue client fails to build, the
   * credentials subtree is closed before propagating so nothing leaks.
   *
   * @param region AWS region of the Glue registry
   * @param registryName Glue registry name
   * @param assumeRoleArn optional role to assume; the SDK default chain is used when null or blank
   */
  public static GlueRegistry create(String region, String registryName, String assumeRoleArn) {
    if (region == null || region.isBlank()) {
      throw new IllegalArgumentException("region is mandatory when schema.registry.type is glue");
    }
    Region awsRegion = Region.of(region.trim());
    AwsCredentials credentials = AwsCredentials.resolve(awsRegion, assumeRoleArn);
    try {
      // Pin the sync HTTP client explicitly (the AWS SDK errors when it finds more than one on the
      // classpath — apache-client + url-connection-client are both present).
      var builder =
          GlueClient.builder().region(awsRegion).httpClient(UrlConnectionHttpClient.create());
      if (credentials.credentials() != null) {
        builder.credentialsProvider(credentials.credentials());
      }
      log.info(
          "Initializing Glue schema registry (region {}, registry {})", awsRegion.id(), registryName);
      return new GlueRegistry(builder.build(), credentials, registryName);
    } catch (RuntimeException e) {
      credentials.close();
      throw e;
    }
  }

  @Override
  public Schema getAvroSchema(String subject, int version) {
    try {
      if (!subject.isEmpty() && version != 0) {
        GetSchemaVersionResponse response =
            this.glue.getSchemaVersion(
                GetSchemaVersionRequest.builder()
                    .schemaId(
                        SchemaId.builder()
                            .registryName(this.registryName)
                            .schemaName(subject)
                            .build())
                    .schemaVersionNumber(
                        SchemaVersionNumber.builder().versionNumber((long) version).build())
                    .build());
        if (response.dataFormat() != DataFormat.AVRO) {
          log.error(
              "Schema data format is not AVRO for schema {}, version {}. Found {}.",
              subject,
              version,
              response.dataFormatAsString());
          return null;
        }
        return new Schema.Parser().parse(response.schemaDefinition());
      }
    } catch (RuntimeException e) {
      log.error("Failed to retrieve the Avro schema for schema {}, version {}", subject, version, e);
    }
    return null;
  }

  @Override
  public String getJsonSchemaString(String subject, int version) {
    // The Glue-framed contract this connector implements covers Avro only.
    log.error(
        "JSON schemas are not supported with schema.registry.type=glue (schema {}, version {})",
        subject,
        version);
    return "";
  }

  @Override
  public void close() {
    closeQuietly(this.credentials);
    closeQuietly(this.glue);
  }

  private static void closeQuietly(AutoCloseable resource) {
    if (resource == null) {
      return;
    }
    try {
      resource.close();
    } catch (Exception e) {
      log.warn("Failed to close {} while releasing the Glue registry", resource.getClass(), e);
    }
  }
}
