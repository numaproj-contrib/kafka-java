package io.numaproj.kafka.metrics;

import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

/**
 * Owns the lifecycle of the Prometheus HTTP scrape endpoint.
 *
 * <p>Default port is {@code 9091}, chosen to avoid Numaflow's reserved {@code 2469} (metrics),
 * {@code 2470} (runtime) and {@code 4327} ports. Overridable via the {@code KAFKA_JAVA_METRICS_PORT}
 * environment variable, and disabled entirely when that variable is set to {@code 0} - matching the
 * existing {@code ROOT_LOG_LEVEL} / {@code KAFKA_LOG_LEVEL} env-var convention.
 */
@Slf4j
public class MetricsServer {

  public static final int DEFAULT_PORT = 9091;
  private static final String PORT_ENV_VAR = "KAFKA_JAVA_METRICS_PORT";

  private final HTTPServer httpServer;

  private MetricsServer(HTTPServer httpServer) {
    this.httpServer = httpServer;
  }

  /**
   * Starts the metrics HTTP server against the given registry, unless disabled via {@code
   * KAFKA_JAVA_METRICS_PORT=0}.
   *
   * @return the running server, or {@code null} if metrics serving is disabled
   */
  public static MetricsServer start(PrometheusRegistry registry) throws IOException {
    int port = resolvePort();
    if (port == 0) {
      log.info("Metrics server disabled ({}=0)", PORT_ENV_VAR);
      return null;
    }
    HTTPServer server =
        HTTPServer.builder().port(port).registry(registry).buildAndStart();
    log.info("Metrics server listening on port {} (path /metrics)", port);
    return new MetricsServer(server);
  }

  private static int resolvePort() {
    String override = System.getenv(PORT_ENV_VAR);
    if (override == null || override.isBlank()) {
      return DEFAULT_PORT;
    }
    try {
      return Integer.parseInt(override.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          PORT_ENV_VAR + " must be an integer, got: " + override, e);
    }
  }

  /** Stops the metrics server, releasing its port. Safe to call more than once. */
  public void stop() {
    log.info("Stopping metrics server");
    httpServer.close();
  }
}
