package io.zeroshift.failures;

import java.util.List;
import java.util.Map;
import tools.jackson.databind.node.ObjectNode;

/**
 * One advanced failure experiment. Every one runs the same five stages in order: Naive design →
 * Failure → Observable consequence → Correct design → Recovery. A stage changes or reads the real
 * system and returns what it found, with the claims it checked ({@code checks}); {@code memo}
 * carries what earlier stages of the same run learned (ids, offsets, timestamps).
 */
public interface FailureLab {
  List<String> STAGES =
      List.of("Naive design", "Failure", "Observable consequence", "Correct design", "Recovery");

  String id();

  String title();

  String summary();

  /** The naive design and the correct one, in a sentence each. */
  String naive();

  String correct();

  /** What each of the five stages does, in order. */
  List<String> plan();

  /** The infrastructure this lab needs, as shown on the page. */
  List<String> requires();

  ObjectNode run(int stage, ObjectNode memo, Trace trace) throws Exception;

  /** The live inspector: what the lab's databases, brokers and processes hold right now. */
  ObjectNode state() throws Exception;

  /** Puts the lab's own infrastructure back to a clean state; touches nothing else. */
  ObjectNode reset() throws Exception;

  /** Span events on the stage's trace (Tempo), next to what the stage returns. */
  interface Trace {
    void event(String name, Map<String, String> attributes);

    String traceId();

    Trace NONE =
        new Trace() {
          @Override
          public void event(String name, Map<String, String> attributes) {}

          @Override
          public String traceId() {
            return null;
          }
        };
  }
}
