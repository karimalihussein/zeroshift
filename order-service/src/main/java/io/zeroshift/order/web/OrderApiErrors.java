package io.zeroshift.order.web;

import io.zeroshift.order.application.CatalogUnavailable;
import io.zeroshift.order.application.ConcurrencyConflict;
import io.zeroshift.order.application.OrderNotFound;
import io.zeroshift.order.application.PlaceOrder;
import io.zeroshift.order.application.UnknownCustomer;
import io.zeroshift.order.application.VoucherExhausted;
import io.zeroshift.order.domain.OrderRuleViolation;
import io.zeroshift.order.domain.VoucherNotApplicable;
import io.zeroshift.platform.web.ApiError;
import io.zeroshift.platform.web.ApiErrors;
import io.zeroshift.platform.web.ApiExceptionHandler;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
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
  public static final String UNKNOWN_CUSTOMER = "UNKNOWN_CUSTOMER";
  public static final String VOUCHER_NOT_APPLICABLE = "VOUCHER_NOT_APPLICABLE";
  public static final String VOUCHER_EXHAUSTED = "VOUCHER_EXHAUSTED";
  public static final String CATALOG_UNAVAILABLE = "CATALOG_UNAVAILABLE";

  /** How long a client should wait before retrying a placement the catalog could not price. */
  static final int CATALOG_RETRY_SECONDS = 2;

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

  @ExceptionHandler(UnknownCustomer.class)
  ResponseEntity<ApiError> unknownCustomer(UnknownCustomer e, HttpServletRequest request) {
    return ApiErrors.response(
        HttpStatus.UNPROCESSABLE_CONTENT, UNKNOWN_CUSTOMER, e.getMessage(), request);
  }

  @ExceptionHandler(VoucherNotApplicable.class)
  ResponseEntity<ApiError> voucherNotApplicable(
      VoucherNotApplicable e, HttpServletRequest request) {
    return ApiErrors.response(
        HttpStatus.UNPROCESSABLE_CONTENT, VOUCHER_NOT_APPLICABLE, e.getMessage(), request);
  }

  /** Lost the race for a limited voucher's last use: the same request will not succeed later. */
  @ExceptionHandler(VoucherExhausted.class)
  ResponseEntity<ApiError> voucherExhausted(VoucherExhausted e, HttpServletRequest request) {
    return ApiErrors.response(HttpStatus.CONFLICT, VOUCHER_EXHAUSTED, e.getMessage(), request);
  }

  /** No prices, no order: a synchronous dependency's failure, passed on as ours. */
  @ExceptionHandler(CatalogUnavailable.class)
  ResponseEntity<ApiError> catalogUnavailable(CatalogUnavailable e, HttpServletRequest request) {
    return ApiErrors.response(
        HttpStatus.SERVICE_UNAVAILABLE,
        CATALOG_UNAVAILABLE,
        e.getMessage(),
        request,
        List.of(),
        Map.of(ApiErrors.RETRY_AFTER_SECONDS, CATALOG_RETRY_SECONDS));
  }

  /** Someone else wrote first; retrying the request reads the new state. */
  @ExceptionHandler(ConcurrencyConflict.class)
  ResponseEntity<ApiError> conflict(ConcurrencyConflict e, HttpServletRequest request) {
    return ApiErrors.response(HttpStatus.CONFLICT, CONCURRENT_UPDATE, e.getMessage(), request);
  }
}
