package io.zeroshift.docs;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Reads {@code infra/kafka/topics.sh}. The script declares partition count and retention through a
 * {@code create} function and the {@code for} loops that call it. The function body is skipped;
 * only the calls are catalogued.
 */
final class TopicScript {
  record Declared(String name, int partitions, Map<String, String> configs) {}

  private static final Pattern REPLICATION = Pattern.compile("--replication-factor\\s+(\\d+)");

  private TopicScript() {}

  static int replicationFactor(String script) {
    var matcher = REPLICATION.matcher(script);
    if (!matcher.find()) throw new IllegalArgumentException("topics.sh sets no replication factor");
    var factor = Integer.parseInt(matcher.group(1));
    if (matcher.find())
      throw new IllegalArgumentException("topics.sh sets more than one replication factor");
    return factor;
  }

  static List<Declared> parse(String script) {
    var topics = new ArrayList<Declared>();
    List<String> loopValues = null;
    String loopVar = null;
    var body = new ArrayList<String>();
    int depth = 0;
    for (var raw : script.split("\n")) {
      var line = raw.trim();
      if (line.isEmpty() || line.startsWith("#")) continue;
      if (depth > 0) {
        depth += braces(line);
        continue;
      }
      if (line.startsWith("create()") || line.startsWith("create ()")) {
        depth = braces(line);
        if (depth == 0) depth = 1;
        continue;
      }
      if (loopVar == null && line.startsWith("for ")) {
        var header = line.replaceFirst("^for\\s+", "").replaceFirst(";\\s*do\\s*$", "");
        var parts = header.split("\\s+in\\s+", 2);
        loopVar = parts[0].trim();
        loopValues = List.of(parts[1].trim().split("\\s+"));
        body.clear();
        continue;
      }
      if (loopVar != null && line.equals("done")) {
        for (var value : loopValues) {
          for (var statement : body) topics.add(create(expand(statement, loopVar, value)));
        }
        loopVar = null;
        loopValues = null;
        continue;
      }
      if (loopVar != null) {
        body.add(line);
        continue;
      }
      if (line.startsWith("create ")) topics.add(create(line));
    }
    if (depth != 0) throw new IllegalArgumentException("topics.sh has an unclosed function");
    if (loopVar != null) throw new IllegalArgumentException("topics.sh has an unclosed for loop");
    return List.copyOf(topics);
  }

  private static int braces(String line) {
    int depth = 0;
    for (int i = 0; i < line.length(); i++) {
      if (line.charAt(i) == '{') depth++;
      else if (line.charAt(i) == '}') depth--;
    }
    return depth;
  }

  private static String expand(String line, String variable, String value) {
    return line.replace("\"$" + variable, "\"" + value).replace("$" + variable, value);
  }

  private static Declared create(String line) {
    var tokens = line.replace("\"", "").split("\\s+");
    if (tokens.length < 3 || !tokens[0].equals("create"))
      throw new IllegalArgumentException("Not a create call: " + line);
    var configs = new LinkedHashMap<String, String>();
    for (var i = 3; i < tokens.length; i++) {
      var pair = tokens[i].split("=", 2);
      configs.put(pair[0], pair.length == 2 ? pair[1] : "");
    }
    return new Declared(tokens[1], Integer.parseInt(tokens[2]), Map.copyOf(configs));
  }
}
