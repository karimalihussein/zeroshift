package io.zeroshift.web;

import io.zeroshift.domain.InvalidAction;
import io.zeroshift.eventlab.LabServices;
import io.zeroshift.platform.web.ApiError;
import io.zeroshift.platform.web.ApiErrors;
import io.zeroshift.platform.web.ApiExceptionHandler;
import io.zeroshift.platform.web.ErrorCodes;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * The migration lab's own failures, in the shared error format; everything else falls through to
 * the shared {@link ApiExceptionHandler}.
 */
@RestControllerAdvice
@Order(ApiExceptionHandler.SERVICE_ADVICE)
public class LabApiErrors {
  /** An action the migration's current stage does not allow, or input a domain rule refuses. */
  @ExceptionHandler(InvalidAction.class)
  ResponseEntity<ApiError> invalid(InvalidAction e, HttpServletRequest request) {
    return ApiErrors.response(HttpStatus.CONFLICT, "INVALID_ACTION", e.getMessage(), request);
  }

  /**
   * A body the domain refuses while it is being read (OrderEdit checks itself) keeps its reason.
   */
  @ExceptionHandler(HttpMessageNotReadableException.class)
  ResponseEntity<ApiError> unreadable(
      HttpMessageNotReadableException e, HttpServletRequest request) {
    return e.getMostSpecificCause() instanceof InvalidAction invalid
        ? ApiErrors.response(
            HttpStatus.BAD_REQUEST, ErrorCodes.VALIDATION_FAILED, invalid.getMessage(), request)
        : ApiErrors.response(
            HttpStatus.BAD_REQUEST,
            ErrorCodes.MALFORMED_REQUEST,
            ApiExceptionHandler.MALFORMED_BODY,
            request);
  }

  /** A service the control plane called refused or failed: the control plane is its gateway. */
  @ExceptionHandler(LabServices.ActionFailed.class)
  ResponseEntity<ApiError> actionFailed(LabServices.ActionFailed e, HttpServletRequest request) {
    return ApiErrors.response(
        HttpStatus.BAD_GATEWAY, ErrorCodes.BAD_GATEWAY, e.getMessage(), request);
  }
}
