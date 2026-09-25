package io.zeroshift.platform.web;

import org.slf4j.MDC;

/**
 * The current request's id and trace id, as the logs see them. The request id is put in the MDC by
 * {@link RequestIdFilter}; the trace id by the OpenTelemetry agent when it is attached.
 */
public final class RequestContext {
  /** MDC key: log lines of a request carry its id under this name. */
  public static final String REQUEST_ID = "request_id";

  /** MDC key the OpenTelemetry agent uses for the current trace. */
  public static final String TRACE_ID = "trace_id";

  public static String requestId() {
    return MDC.get(REQUEST_ID);
  }

  public static String traceId() {
    return MDC.get(TRACE_ID);
  }

  private RequestContext() {}
}
