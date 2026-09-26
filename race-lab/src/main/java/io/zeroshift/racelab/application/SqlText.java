package io.zeroshift.racelab.application;

import java.util.List;

/** SQL as the timeline shows it: parameters inlined, whitespace collapsed. Display only. */
final class SqlText {
  private SqlText() {}

  static String render(String sql, List<Object> params) {
    var out = new StringBuilder();
    int p = 0;
    for (char c : sql.toCharArray()) {
      if (c == '?' && p < params.size()) out.append(literal(params.get(p++)));
      else out.append(c);
    }
    return out.toString().replaceAll("\\s+", " ").trim();
  }

  private static String literal(Object v) {
    if (v == null) return "NULL";
    if (v instanceof Number || v instanceof Boolean) return v.toString();
    return "'" + v.toString().replace("'", "''") + "'";
  }
}
