package io.zeroshift.order.web;

import io.zeroshift.order.application.ConcurrencyConflict;
import io.zeroshift.order.application.OrderNotFound;
import io.zeroshift.order.application.PlaceOrder;
import io.zeroshift.order.domain.OrderRuleViolation;
import io.zeroshift.platform.web.ApiError;
import io.zeroshift.platform.web.ApiErrors;
import io.zeroshift.platform.web.ApiExceptionHandler;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** The order service's domain exceptions, as API errors with stable codes. */
@RestControllerAdvice
@Order(ApiExceptionHandler.SERVICE_ADVICE)
public class OrderApiErrors {
  public static final String ORDER_NOT_FOUND = "ORDER_NOT_FOUND";
  public static final String ORDER_RULE_VIOLATION = "ORDER_RULE_VIOLATION";
  public static final String IDEMPOTENCY_KEY_REUSED = "IDEMPOTENCY_KEY_REUSED";
  public static final String CONCURRENT_UPDATE = "CONCURRENT_UPDATE";

  @ExceptionHandler(OrderNotFound.class)
  ResponseEntity<ApiError> notFound(OrderNotFound e, HttpServletRequest request) {
    return ApiErrors.response(HttpStatus.NOT_FOUND, ORDER_NOT_FOUND, e.getMessage(), request);
  }

  /** Well-formed but not acceptable: an unknown SKU, an order in the wrong state. */
  @ExceptionHandler(OrderRuleViolation.class)
  ResponseEntity<ApiError> rule(OrderRuleViolation e, HttpServletRequest request) {
    return ApiErrors.response(
        HttpStatus.UNPROCESSABLE_CONTENT, ORDER_RULE_VIOLATION, e.getMessage(), request);
  }

  @ExceptionHandler(PlaceOrder.IdempotencyConflict.class)
  ResponseEntity<ApiError> keyReused(PlaceOrder.IdempotencyConflict e, HttpServletRequest request) {
    return ApiErrors.response(
        HttpStatus.UNPROCESSABLE_CONTENT, IDEMPOTENCY_KEY_REUSED, e.getMessage(), request);
  }

  /** Someone else wrote first; retrying the request reads the new state. */
  @ExceptionHandler(ConcurrencyConflict.class)
  ResponseEntity<ApiError> conflict(ConcurrencyConflict e, HttpServletRequest request) {
    return ApiErrors.response(HttpStatus.CONFLICT, CONCURRENT_UPDATE, e.getMessage(), request);
  }
}
