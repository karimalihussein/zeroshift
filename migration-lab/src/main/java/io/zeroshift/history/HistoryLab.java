package io.zeroshift.history;

import java.util.List;
import tools.jackson.databind.node.ObjectNode;

/**
 * One events-over-time experiment. Every one has the same six steps, run in order: History →
 * Inspect → Change/Rebuild → Replay → Compare → Understand. A step reads or changes the real system
 * and returns what it found; {@code memo} carries what earlier steps of the same run learned (the
 * order chosen, the offsets read, the event written).
 */
public interface HistoryLab {
  List<String> STEPS =
      List.of("History", "Inspect", "Change / rebuild", "Replay", "Compare", "Understand");

  String id();

  String title();

  String summary();

  /** What each of the six steps does, in order. */
  List<String> plan();

  ObjectNode run(int step, ObjectNode memo) throws Exception;
}
