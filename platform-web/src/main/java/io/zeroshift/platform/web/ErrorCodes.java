package io.zeroshift.platform.web;

import org.springframework.http.HttpStatusCode;

/**
 * Error codes every service may return. They are part of the API: clients branch on them, so they
 * never change meaning. Service-specific codes are declared next to the exceptions they describe.
 */
public final class ErrorCodes {
  public static final String VALIDATION_FAILED = "VALIDATION_FAILED";
  public static final String MALFORMED_REQUEST = "MALFORMED_REQUEST";
  public static final String INVALID_PARAMETER = "INVALID_PARAMETER";
  public static final String NOT_FOUND = "NOT_FOUND";
  public static final String METHOD_NOT_ALLOWED = "METHOD_NOT_ALLOWED";
  public static final String UNSUPPORTED_MEDIA_TYPE = "UNSUPPORTED_MEDIA_TYPE";
  public static final String CONFLICT = "CONFLICT";
  public static final String BAD_GATEWAY = "BAD_GATEWAY";
  public static final String INTERNAL_ERROR = "INTERNAL_ERROR";

  /** The generic code for an HTTP status, when nothing more specific is known. */
  public static String forStatus(HttpStatusCode status) {
    return switch (status.value()) {
      case 400 -> INVALID_PARAMETER;
      case 404 -> NOT_FOUND;
      case 405 -> METHOD_NOT_ALLOWED;
      case 409 -> CONFLICT;
      case 415 -> UNSUPPORTED_MEDIA_TYPE;
      case 502 -> BAD_GATEWAY;
      default -> status.is5xxServerError() ? INTERNAL_ERROR : "HTTP_" + status.value();
    };
  }

  private ErrorCodes() {}
}
