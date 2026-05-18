package io.numaproj.kafka.common;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AwsIamRoleUtils {
  private static final String AWS_ROLE_ARN_KEY = "aws.role.arn";
  private static final String AWS_IAM_SESSION_NAME = "numaflow-kafka-consumer";

  public static void assumeRoleIfConfigured(Properties props) {
    String roleArn = props.getProperty(AWS_ROLE_ARN_KEY);
    props.remove(AWS_ROLE_ARN_KEY);
    if (roleArn == null || roleArn.isBlank()) {
      return;
    }

    StsClient stsClient = StsClient.builder()
        // Use pure java http client instead of Netty (default) which conflicts with project dependencies
        .httpClientBuilder(UrlConnectionHttpClient.builder())
        .build();
    AssumeRoleRequest request = AssumeRoleRequest.builder()
      .roleArn(roleArn)
      .roleSessionName(AWS_IAM_SESSION_NAME)
      .build();

    Credentials creds = applyCredentials(stsClient, request, roleArn);
    scheduleCredentialRefresh(stsClient, request, roleArn, creds.expiration());
  }

  private static Credentials applyCredentials(StsClient stsClient, AssumeRoleRequest request, String roleArn) {
    log.info("Assuming AWS role: {}", roleArn);
    Credentials creds = stsClient.assumeRole(request).credentials();

    // Set STS credentials as system properties, which have the highest priority in the credential provider chain
    System.setProperty("aws.accessKeyId", creds.accessKeyId());
    System.setProperty("aws.secretAccessKey", creds.secretAccessKey());
    System.setProperty("aws.sessionToken", creds.sessionToken());

    log.info("Successfully assumed role: {}, credentials expire at: {}", roleArn, creds.expiration());
    return creds;
  }

  private static void scheduleCredentialRefresh(StsClient stsClient, AssumeRoleRequest request,
    String roleArn, Instant expiration) {

    // refresh 5 minutes before expiry
    long refreshInSeconds = Math.max(0, expiration.getEpochSecond() - Instant.now().getEpochSecond() - 300);
    log.info("Scheduling AWS credential refresh in {}s for role: {}", refreshInSeconds, roleArn);
    var scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "aws-creds-refresh");
      // mark as daemon thread so that it doesn't prevent JVM shutdown
      t.setDaemon(true);
      return t;
    });

    // use scheduleWithFixedDelay so each refresh interval is computed from the previous completion
    scheduler.scheduleWithFixedDelay(() -> {
      try {
        applyCredentials(stsClient, request, roleArn);
      } catch (Exception e) {
        log.error("Failed to refresh AWS credentials for role: {}", roleArn, e);
      }
    }, refreshInSeconds, refreshInSeconds, TimeUnit.SECONDS);
  }
}
