package io.numaproj.kafka.encryption.aws;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

/**
 * The AWS credentials the KMS client uses. Either the SDK default chain (IRSA / env / etc.) or, when
 * an {@code assumeRoleArn} is configured, temporary credentials from STS AssumeRole.
 *
 * <p>Depends on {@link StsClient} and the role ARN. When it creates them, it owns the STS client and
 * the assume-role provider, and releases them in {@link #close()} (STS client first, then the
 * provider that used it).
 */
@Slf4j
class DekCredentialsProvider implements AutoCloseable {

  private static final String SESSION_NAME = "kafka-java-kms";

  private final AwsCredentialsProvider credentials; // null => SDK default chain
  private final AutoCloseable ownedProvider; // the assume-role provider, as a closeable (nullable)
  private final AutoCloseable ownedStsClient; // the STS client backing the provider (nullable)

  DekCredentialsProvider(
      AwsCredentialsProvider credentials, AutoCloseable ownedProvider, AutoCloseable ownedStsClient) {
    this.credentials = credentials;
    this.ownedProvider = ownedProvider;
    this.ownedStsClient = ownedStsClient;
  }

  /** SDK default credential chain; owns nothing. */
  static DekCredentialsProvider defaultChain() {
    return new DekCredentialsProvider(null, null, null);
  }

  /**
   * Temporary credentials via STS AssumeRole; owns the STS client and provider it builds. If
   * construction throws partway, the STS client is closed before propagating.
   */
  static DekCredentialsProvider assumeRole(Region region, String assumeRoleArn) {
    StsClient sts = null;
    StsAssumeRoleCredentialsProvider provider = null;
    try {
      // Pin the sync HTTP client explicitly (the AWS SDK errors when it finds more than one on the
      // classpath — apache-client + url-connection-client are both present).
      sts = StsClient.builder().region(region).httpClient(UrlConnectionHttpClient.create()).build();
      provider =
          StsAssumeRoleCredentialsProvider.builder()
              .stsClient(sts)
              .refreshRequest(
                  AssumeRoleRequest.builder()
                      .roleArn(assumeRoleArn.trim())
                      .roleSessionName(SESSION_NAME)
                      .build())
              .build();
      return new DekCredentialsProvider(provider, provider, sts);
    } catch (RuntimeException e) {
      closeQuietly(sts);
      closeQuietly(provider);
      throw e;
    }
  }

  /** The credentials for the KMS client builder, or {@code null} to use the SDK default chain. */
  AwsCredentialsProvider credentials() {
    return this.credentials;
  }

  @Override
  public void close() {
    closeQuietly(this.ownedStsClient);
    closeQuietly(this.ownedProvider);
  }

  private static void closeQuietly(AutoCloseable resource) {
    if (resource == null) {
      return;
    }
    try {
      resource.close();
    } catch (Exception e) {
      log.warn("Failed to close {} while releasing AWS credentials", resource.getClass(), e);
    }
  }
}
