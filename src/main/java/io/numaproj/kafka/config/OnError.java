package io.numaproj.kafka.config;

import java.util.Locale;

/**
 * The source's behavior when a record fails to be read (either decoded or converted). A {@code
 * user.configuration} key, source-only - see {@link UserConfig}.
 */
public enum OnError {
  /** Propagate the failure so the vertex crashes visibly and diagnosably. The default. */
  FAIL,
  /** Drop the failing record, count it, and continue. See the source's metrics documentation. */
  SKIP;

  /**
   * Parses the {@code onError} configuration value, case-insensitively.
   *
   * @param value the configured value; blank or {@code null} means {@link #FAIL}
   * @return the parsed policy
   * @throws IllegalArgumentException if {@code value} is non-blank and not a recognized policy -
   *     rejecting a typo at startup rather than silently defaulting to {@link #FAIL}
   */
  public static OnError from(String value) {
    if (value == null || value.isBlank()) {
      return FAIL;
    }
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid onError value: '" + value + "'. Must be one of: fail, skip", e);
    }
  }
}
