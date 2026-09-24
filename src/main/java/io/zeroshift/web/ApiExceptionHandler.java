package io.zeroshift.web;

import io.zeroshift.domain.*;
import org.slf4j.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

@RestControllerAdvice
public class ApiExceptionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ApiExceptionHandler.class);

  @ExceptionHandler(InvalidAction.class)
  public ResponseEntity<ProblemDetail> invalid(InvalidAction e) {
    return ResponseEntity.status(409)
        .body(ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage()));
  }

  @ExceptionHandler(org.springframework.http.converter.HttpMessageNotReadableException.class)
  public ResponseEntity<ProblemDetail> invalidBody(
      org.springframework.http.converter.HttpMessageNotReadableException e) {
    String detail =
        e.getMostSpecificCause() instanceof InvalidAction invalid
            ? invalid.getMessage()
            : "Provide a valid customer name, status and decimal amount";
    return ResponseEntity.badRequest()
        .body(ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detail));
  }

  @ExceptionHandler(org.springframework.beans.TypeMismatchException.class)
  public ResponseEntity<ProblemDetail> typeMismatch(
      org.springframework.beans.TypeMismatchException e) {
    return ResponseEntity.badRequest()
        .body(
            ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, "Invalid value for " + e.getPropertyName()));
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ProblemDetail> failure(Exception e) {
    // Routing errors (404, 405, 415…) keep their HTTP status instead of reading as database
    // failures.
    if (e instanceof org.springframework.web.ErrorResponse response)
      return ResponseEntity.status(response.getStatusCode()).body(response.getBody());
    LOG.error("Dashboard action failed", e);
    return ResponseEntity.internalServerError()
        .body(
            ProblemDetail.forStatusAndDetail(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Database operation failed. See the application logs; the last committed checkpoint is retained."));
  }
}
