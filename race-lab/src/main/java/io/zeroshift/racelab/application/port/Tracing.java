package io.zeroshift.racelab.application.port;

import java.util.Map;

/**
 * Spans for a run and for each transaction attempt, so a timeline event leads to its trace: the
 * run, the request's transaction, and the JDBC statements (with their lock waits) inside it.
 */
public interface Tracing {
  /** Starts a span as a child of the current one and makes it current on this thread. */
  TraceSpan start(String name, Map<String, String> attributes);

  /** Carries the current trace context into a task run on another thread. */
  Runnable propagate(Runnable task);

  interface TraceSpan extends AutoCloseable {
    /** Null when no tracer is attached. */
    String traceId();

    String spanId();

    void attribute(String key, String value);

    /** A timestamped event on the span (a lock wait beginning or ending). Thread-safe. */
    void event(String name, Map<String, String> attributes);

    void error(String message);

    /** Ends the span and restores the previous current span on this thread. */
    @Override
    void close();
  }

  Tracing NONE =
      new Tracing() {
        @Override
        public TraceSpan start(String name, Map<String, String> attributes) {
          return new TraceSpan() {
            public String traceId() {
              return null;
            }

            public String spanId() {
              return null;
            }

            public void attribute(String key, String value) {}

            public void event(String name, Map<String, String> attributes) {}

            public void error(String message) {}

            public void close() {}
          };
        }

        @Override
        public Runnable propagate(Runnable task) {
          return task;
        }
      };
}
