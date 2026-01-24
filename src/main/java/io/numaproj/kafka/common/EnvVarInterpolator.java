package io.numaproj.kafka.common;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

/**
 * Expands ${ENV_VAR} placeholders in Java {@link Properties} values.
 *
 * <p>Behavior:
 *
 * <ul>
 *   <li>Only expands placeholders in the form {@code ${VARNAME}}
 *   <li>If an env var is missing, the placeholder is left unchanged
 * </ul>
 */
@Slf4j
public final class EnvVarInterpolator {

  private static final Pattern ENV_PLACEHOLDER =
      Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)\\}");

  private EnvVarInterpolator() {}

  /** Interpolate using {@link System#getenv()}. */
  public static void interpolate(Properties props) {
    interpolate(props, System.getenv());
  }

  /** Interpolate using provided env map (useful for tests). */
  public static void interpolate(Properties props, Map<String, String> env) {
    if (props == null || props.isEmpty()) {
      return;
    }
    if (env == null || env.isEmpty()) {
      // Still do a pass to keep behavior consistent (placeholders remain unchanged).
      env = Map.of();
    }

    for (String name : props.stringPropertyNames()) {
      String raw = props.getProperty(name);
      if (raw == null || raw.isEmpty()) {
        continue;
      }
      String expanded = expand(raw, env);
      if (!raw.equals(expanded)) {
        props.setProperty(name, expanded);
        log.debug("Interpolated property key='{}'", name);
      }
    }
  }

  private static String expand(String value, Map<String, String> env) {
    Matcher matcher = ENV_PLACEHOLDER.matcher(value);
    if (!matcher.find()) {
      return value;
    }

    matcher.reset();
    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      String varName = matcher.group(1);
      String envVal = env.get(varName);
      if (envVal == null) {
        // Leave placeholder unchanged.
        matcher.appendReplacement(sb, Matcher.quoteReplacement(matcher.group(0)));
      } else {
        matcher.appendReplacement(sb, Matcher.quoteReplacement(envVal));
      }
    }
    matcher.appendTail(sb);
    return sb.toString();
  }
}

