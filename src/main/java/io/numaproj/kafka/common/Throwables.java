package io.numaproj.kafka.common;

/** Utility methods for exception cause-chain inspection. */
public final class Throwables {

  private Throwables() {}

  public static boolean hasCauseOfType(Throwable failure, Class<?> type) {
    for (Throwable t = failure; t != null; t = t.getCause()) {
      if (type.isInstance(t)) {
        return true;
      }
    }
    return false;
  }
}
