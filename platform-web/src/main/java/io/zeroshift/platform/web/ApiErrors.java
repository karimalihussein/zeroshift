package io.zeroshift.platform.web;

import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/** Builds {@link ApiError} responses, filling in what every error carries. */
public final class ApiErrors {
  /** Context key: also sent as the {@code Retry-After} header. */
  public static final String RETRY_AFTER_SECONDS = "retryAfterSeconds";

  public static ResponseEntity<ApiError> response(
      HttpStatusCode status, String code, String detail, HttpServletRequest request) {
    return response(status, code, detail, request, List.of(), Map.of());
  }

  public static ResponseEntity<ApiError> response(
      HttpStatusCode status,
      String code,
      String detail,
      HttpServletRequest request,
      List<ApiError.FieldViolation> errors,
      Map<String, Object> context) {
    var reason = HttpStatus.resolve(status.value());
    var builder = ResponseEntity.status(status);
    // A refusal that says when to come back (429, 503) says it in the standard header too.
    if (context.get(RETRY_AFTER_SECONDS) instanceof Integer seconds)
      builder.header(HttpHeaders.RETRY_AFTER, seconds.toString());
    return builder
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .body(
            new ApiError(
                "urn:zeroshift:error:" + code.toLowerCase().replace('_', '-'),
                reason == null ? "HTTP " + status.value() : reason.getReasonPhrase(),
                status.value(),
                detail,
                request == null ? null : request.getRequestURI(),
                code,
                RequestContext.requestId(),
                RequestContext.traceId(),
                errors,
                context));
  }

  private ApiErrors() {}
}
