package io.zeroshift.failures;

import java.util.Objects;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * The claims a stage checked against what it measured. A stage whose claim does not hold is not
 * counted as done: the runner reports it and it can be run again.
 */
final class Checks {
  private Checks() {}

  static void check(ObjectNode result, String claim, Object expected, Object observed) {
    add(
        result,
        claim,
        String.valueOf(expected),
        String.valueOf(observed),
        Objects.equals(expected, observed));
  }

  static void check(ObjectNode result, String claim, String expected, Object observed, boolean ok) {
    add(result, claim, expected, String.valueOf(observed), ok);
  }

  private static void add(
      ObjectNode result, String claim, String expected, String observed, boolean ok) {
    ArrayNode checks =
        result.has("checks") ? (ArrayNode) result.get("checks") : result.putArray("checks");
    checks
        .addObject()
        .put("claim", claim)
        .put("expected", expected)
        .put("observed", observed)
        .put("ok", ok);
  }

  /** Adds one row to the naive-versus-correct comparison the page builds up stage by stage. */
  static void compare(ObjectNode result, String aspect, Object naive, Object correct) {
    ArrayNode rows =
        result.has("compare") ? (ArrayNode) result.get("compare") : result.putArray("compare");
    rows.addObject()
        .put("aspect", aspect)
        .put("naive", naive == null ? null : String.valueOf(naive))
        .put("correct", correct == null ? null : String.valueOf(correct));
  }

  static boolean allHold(ObjectNode result) {
    if (!result.has("checks")) return true;
    for (var c : result.get("checks")) if (!c.path("ok").asBoolean()) return false;
    return true;
  }

  static String failures(ObjectNode result) {
    var text = new StringBuilder();
    for (var c : result.path("checks"))
      if (!c.path("ok").asBoolean())
        text.append(text.isEmpty() ? "" : "; ")
            .append(c.path("claim").asString())
            .append(": expected ")
            .append(c.path("expected").asString())
            .append(", observed ")
            .append(c.path("observed").asString());
    return text.toString();
  }
}
