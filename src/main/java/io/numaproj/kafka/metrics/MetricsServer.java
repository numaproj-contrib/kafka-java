package io.numaproj.kafka.metrics;

import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

/**
 * Owns the lifecycle of the Prometheus HTTP scrape endpoint.
 *
 * <p>Default port is {@code 9091}, which avoids Numaflow's reserved {@code 2469} (metrics), {@code
 * 2470} (runtime) and {@code 4327} ports. Set {@code KAFKA_JAVA_METRICS_PORT} to serve on another
 * port, or to {@code 0} to disable the endpoint.
 */
@Slf4j
public class MetricsServer {

  public static final int DEFAULT_PORT = 9091;
  private static final String PORT_ENV_VAR = "KAFKA_JAVA_METRICS_PORT";

  /** Sentinel returned when metrics serving is disabled; {@link #stop()} is a no-op. */
  private static final MetricsServer DISABLED = new MetricsServer(null);

  private final HTTPServer httpServer;

  private MetricsServer(HTTPServer httpServer) {
    this.httpServer = httpServer;
  }

  /**
   * Starts the metrics HTTP server against {@code PrometheusRegistry.defaultRegistry}, unless
   * disabled via {@code KAFKA_JAVA_METRICS_PORT=0}.
   *
   * @return the running server, or a no-op sentinel if metrics serving is disabled
   */
  public static MetricsServer start() throws IOException {
    int port = resolvePort();
    if (port == 0) {
      log.info("Metrics server disabled ({}=0)", PORT_ENV_VAR);
      return DISABLED;
    }
    HTTPServer server =
        HTTPServer.builder().port(port).registry(PrometheusRegistry.defaultRegistry).buildAndStart();
    log.info("Metrics server listening on port {} (path /metrics)", port);
    return new MetricsServer(server);
  }

  private static int resolvePort() {
    String override = System.getenv(PORT_ENV_VAR);
    if (override == null || override.isBlank()) {
      return DEFAULT_PORT;
    }
    int port;
    try {
      port = Integer.parseInt(override.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          PORT_ENV_VAR + " must be an integer, got: " + override, e);
    }
    if (port < 0 || port > 65535) {
      throw new IllegalArgumentException(
          PORT_ENV_VAR + " must be between 0 (disabled) and 65535, got: " + override);
    }
    return port;
  }

  /** Stops the metrics server, releasing its port. Safe to call more than once. No-op if disabled. */
  public void stop() {
    if (httpServer == null) {
      return;
    }
    log.info("Stopping metrics server");
    httpServer.close();
  }
}
