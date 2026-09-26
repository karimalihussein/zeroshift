package io.zeroshift.racelab.experiments;

import io.zeroshift.racelab.domain.ExperimentInfo.ModeInfo;
import io.zeroshift.racelab.domain.Isolation;
import io.zeroshift.racelab.domain.Mode;
import io.zeroshift.racelab.domain.RunConfig;
import io.zeroshift.racelab.domain.RunResult.Explanation;
import io.zeroshift.racelab.domain.RunResult.Outcome;
import io.zeroshift.racelab.domain.RunResult.RequestResult;
import java.util.List;
import java.util.Map;

/** Small helpers the experiments share for their texts and numbers. */
final class Texts {
  private Texts() {}

  static ModeInfo mode(Mode mode, String how, String sql, Isolation isolation, boolean fixes) {
    return new ModeInfo(mode, mode.label(), how, sql, isolation, fixes);
  }

  static long number(Map<String, Object> row, String column) {
    var v = row.get(column);
    return v == null ? 0 : ((Number) v).longValue();
  }

  static int count(List<RequestResult> requests, Outcome outcome) {
    return (int) requests.stream().filter(r -> r.outcome() == outcome).count();
  }

  static String plural(long n, String word) {
    return n + " " + word + (n == 1 ? "" : "s");
  }

  static String lanes(List<RequestResult> requests, Outcome outcome) {
    var lanes = requests.stream().filter(r -> r.outcome() == outcome).map(RequestResult::lane).toList();
    return lanes.isEmpty() ? "none" : String.join(", ", lanes);
  }

  static Explanation.Fix fix(Mode mode, Isolation isolation, String why) {
    return new Explanation.Fix(mode, isolation, "Run it again with " + mode.label()
        + (isolation != null && mode != Mode.SERIALIZABLE ? " at " + isolation.sql() : ""), why);
  }

  /** How the run went, for strategies that kept the invariant. */
  static String preserved(RunConfig config, List<RequestResult> requests) {
    int ok = count(requests, Outcome.SUCCEEDED);
    int declined = count(requests, Outcome.REJECTED);
    int aborted = count(requests, Outcome.ABORTED);
    int retried = (int) requests.stream().filter(r -> r.attempts() > 1).count();
    return plural(ok, "request")
        + " succeeded ("
        + lanes(requests, Outcome.SUCCEEDED)
        + ")"
        + (declined > 0 ? ", " + declined + " declined (" + lanes(requests, Outcome.REJECTED) + ")" : "")
        + (aborted > 0 ? ", " + aborted + " aborted (" + lanes(requests, Outcome.ABORTED) + ")" : "")
        + (retried > 0 ? "; " + plural(retried, "request") + " retried" : "")
        + ".";
  }
}
