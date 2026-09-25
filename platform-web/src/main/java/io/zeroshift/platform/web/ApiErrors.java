package io.zeroshift.platform.web;

import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/** Builds {@link ApiError} responses, filling in what every error carries. */
public final class ApiErrors {
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
    return ResponseEntity.status(status)
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
