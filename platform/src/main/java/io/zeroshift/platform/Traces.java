package io.zeroshift.platform;

import io.opentelemetry.api.trace.Span;

/** The current W3C trace context, when the OpenTelemetry agent is attached; null otherwise. */
public final class Traces {
  public static String traceparent() {
    var context = Span.current().getSpanContext();
    return context.isValid()
        ? "00-"
            + context.getTraceId()
            + "-"
            + context.getSpanId()
            + "-"
            + context.getTraceFlags().asHex()
        : null;
  }

  public static String traceId() {
    var context = Span.current().getSpanContext();
    return context.isValid() ? context.getTraceId() : null;
  }

  private Traces() {}
}
