package io.zeroshift.platform.web;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;

/**
 * Every error response, served as {@code application/problem+json}: the RFC 9457 fields ({@code
 * type}, {@code title}, {@code status}, {@code detail}, {@code instance}) plus a stable,
 * machine-readable {@code code}, the request and trace ids to find it in the logs, field-level
 * {@code errors} for invalid input, and operation-specific facts in {@code context}.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record ApiError(
    String type,
    String title,
    int status,
    String detail,
    String instance,
    String code,
    String requestId,
    String traceId,
    List<FieldViolation> errors,
    Map<String, Object> context) {

  /** One invalid input: which field or parameter, and why. */
  public record FieldViolation(String field, String message) {}
}
