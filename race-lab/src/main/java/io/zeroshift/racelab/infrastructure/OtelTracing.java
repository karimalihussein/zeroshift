package io.zeroshift.racelab.infrastructure;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.zeroshift.racelab.application.port.Tracing;
import java.util.Map;

/**
 * Spans through the OpenTelemetry API. With the Java agent attached (Compose), they join the
 * request's trace and the agent's JDBC spans nest inside each transaction span; without it the API
 * is a no-op and trace ids are null.
 */
public final class OtelTracing implements Tracing {
  private final Tracer tracer = GlobalOpenTelemetry.getTracer("io.zeroshift.race-lab");

  @Override
  public TraceSpan start(String name, Map<String, String> attributes) {
    var builder = tracer.spanBuilder(name);
    attributes.forEach(builder::setAttribute);
    var span = builder.startSpan();
    var scope = span.makeCurrent();
    return new TraceSpan() {
      @Override
      public String traceId() {
        return span.getSpanContext().isValid() ? span.getSpanContext().getTraceId() : null;
      }

      @Override
      public String spanId() {
        return span.getSpanContext().isValid() ? span.getSpanContext().getSpanId() : null;
      }

      @Override
      public void attribute(String key, String value) {
        span.setAttribute(key, value);
      }

      @Override
      public void event(String name, Map<String, String> attributes) {
        var builder = io.opentelemetry.api.common.Attributes.builder();
        attributes.forEach(builder::put);
        span.addEvent(name, builder.build());
      }

      @Override
      public void error(String message) {
        span.setStatus(StatusCode.ERROR, message);
      }

      @Override
      public void close() {
        scope.close();
        span.end();
      }
    };
  }

  @Override
  public Runnable propagate(Runnable task) {
    return Context.current().wrap(task);
  }
}
