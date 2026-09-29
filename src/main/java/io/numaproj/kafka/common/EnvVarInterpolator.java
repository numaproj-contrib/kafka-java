package io.numaproj.kafka.common;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
 *   <li>If any env var is missing, throws {@link IllegalArgumentException} naming the unresolved
 *       variable(s) and the property key(s) that reference them
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

    List<String> unresolved = new ArrayList<>();
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
      // Collect any placeholders that survive expansion (env var not set).
      Set<String> remaining = new LinkedHashSet<>();
      Matcher m = ENV_PLACEHOLDER.matcher(expanded);
      while (m.find()) {
        remaining.add(m.group(0));
      }
      for (String placeholder : remaining) {
        unresolved.add(placeholder + " in property '" + name + "'");
      }
    }
    if (!unresolved.isEmpty()) {
      throw new IllegalArgumentException(
          "Unresolved environment variable(s) in config: " + String.join(", ", unresolved));
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
        matcher.appendReplacement(sb, Matcher.quoteReplacement(matcher.group(0)));
      } else {
        matcher.appendReplacement(sb, Matcher.quoteReplacement(envVal));
      }
    }
    matcher.appendTail(sb);
    return sb.toString();
  }
}

