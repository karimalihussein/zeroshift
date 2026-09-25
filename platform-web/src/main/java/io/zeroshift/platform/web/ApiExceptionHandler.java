package io.zeroshift.platform.web;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.MethodParameter;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.method.ParameterErrors;
import org.springframework.web.ErrorResponse;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/**
 * The one place framework and boundary exceptions become {@link ApiError}s. It runs last: a
 * service's own advice ({@code @Order(ApiExceptionHandler.SERVICE_ADVICE)}) translates that
 * service's domain exceptions first, and everything else lands here.
 */
@RestControllerAdvice
@Order(Ordered.LOWEST_PRECEDENCE)
public class ApiExceptionHandler {
  /** Order for a service's own advice, so it is consulted before this one. */
  public static final String MALFORMED_BODY =
      "The request body is missing or is not valid JSON for this operation";

  public static final int SERVICE_ADVICE = 0;

  private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

  @ExceptionHandler(ApiException.class)
  ResponseEntity<ApiError> api(ApiException e, HttpServletRequest request) {
    return ApiErrors.response(
        e.status(), e.code(), e.getMessage(), request, List.of(), e.context());
  }

  /** An invalid @Valid request body. */
  @ExceptionHandler(MethodArgumentNotValidException.class)
  ResponseEntity<ApiError> invalidBody(
      MethodArgumentNotValidException e, HttpServletRequest request) {
    var errors =
        Stream.concat(
                e.getBindingResult().getFieldErrors().stream()
                    .map(f -> new ApiError.FieldViolation(f.getField(), f.getDefaultMessage())),
                e.getBindingResult().getGlobalErrors().stream()
                    .map(
                        g -> new ApiError.FieldViolation(g.getObjectName(), g.getDefaultMessage())))
            .toList();
    return invalid(errors, request);
  }

  /** Constraints on parameters (@RequestParam, @PathVariable, @RequestHeader) or a body. */
  @ExceptionHandler(HandlerMethodValidationException.class)
  ResponseEntity<ApiError> invalidParameters(
      HandlerMethodValidationException e, HttpServletRequest request) {
    var errors =
        e.getParameterValidationResults().stream()
            .flatMap(
                result ->
                    // A @Valid body validated alongside constrained parameters: name its fields.
                    result instanceof ParameterErrors body
                        ? body.getFieldErrors().stream()
                            .map(
                                error ->
                                    new ApiError.FieldViolation(
                                        error.getField(), error.getDefaultMessage()))
                        : result.getResolvableErrors().stream()
                            .map(
                                error ->
                                    new ApiError.FieldViolation(
                                        clientName(result.getMethodParameter()),
                                        error.getDefaultMessage())))
            .toList();
    return invalid(errors, request);
  }

  /** The name the client used: a header's or query parameter's declared name, if it has one. */
  private static String clientName(MethodParameter parameter) {
    var header = parameter.getParameterAnnotation(RequestHeader.class);
    if (header != null && !header.name().isEmpty()) return header.name();
    var param = parameter.getParameterAnnotation(RequestParam.class);
    if (param != null && !param.name().isEmpty()) return param.name();
    return parameter.getParameterName();
  }

  @ExceptionHandler(ConstraintViolationException.class)
  ResponseEntity<ApiError> constraintViolations(
      ConstraintViolationException e, HttpServletRequest request) {
    var errors =
        e.getConstraintViolations().stream()
            .map(
                v ->
                    new ApiError.FieldViolation(
                        leaf(v.getPropertyPath().toString()), v.getMessage()))
            .toList();
    return invalid(errors, request);
  }

  @ExceptionHandler(HttpMessageNotReadableException.class)
  ResponseEntity<ApiError> unreadable(
      HttpMessageNotReadableException e, HttpServletRequest request) {
    return ApiErrors.response(
        HttpStatus.BAD_REQUEST, ErrorCodes.MALFORMED_REQUEST, MALFORMED_BODY, request);
  }

  @ExceptionHandler({
    MissingServletRequestParameterException.class,
    MissingRequestHeaderException.class
  })
  ResponseEntity<ApiError> missing(Exception e, HttpServletRequest request) {
    var name =
        e instanceof MissingServletRequestParameterException p
            ? p.getParameterName()
            : ((MissingRequestHeaderException) e).getHeaderName();
    return ApiErrors.response(
        HttpStatus.BAD_REQUEST,
        ErrorCodes.INVALID_PARAMETER,
        name + " is required",
        request,
        List.of(new ApiError.FieldViolation(name, "is required")),
        Map.of());
  }

  @ExceptionHandler(MethodArgumentTypeMismatchException.class)
  ResponseEntity<ApiError> typeMismatch(
      MethodArgumentTypeMismatchException e, HttpServletRequest request) {
    var expected =
        e.getRequiredType() == null ? "another type" : e.getRequiredType().getSimpleName();
    return ApiErrors.response(
        HttpStatus.BAD_REQUEST,
        ErrorCodes.INVALID_PARAMETER,
        e.getName() + " must be a valid " + expected,
        request,
        List.of(new ApiError.FieldViolation(e.getName(), "must be a valid " + expected)),
        Map.of());
  }

  /**
   * Everything else. Spring's own HTTP errors (no route 404, 405, 415, ResponseStatusException)
   * keep their status; anything unexpected is a 500 whose cause stays in the log, not the body.
   */
  @ExceptionHandler(Exception.class)
  ResponseEntity<ApiError> unexpected(Exception e, HttpServletRequest request) {
    if (e instanceof ErrorResponse framework)
      return ApiErrors.response(
          framework.getStatusCode(),
          ErrorCodes.forStatus(framework.getStatusCode()),
          framework.getBody().getDetail(),
          request);
    log.error("Unhandled error on {} {}", request.getMethod(), request.getRequestURI(), e);
    return ApiErrors.response(
        HttpStatus.INTERNAL_SERVER_ERROR,
        ErrorCodes.INTERNAL_ERROR,
        "Unexpected error. The request id identifies it in the service's logs.",
        request);
  }

  private static ResponseEntity<ApiError> invalid(
      List<ApiError.FieldViolation> errors, HttpServletRequest request) {
    return ApiErrors.response(
        HttpStatus.BAD_REQUEST,
        ErrorCodes.VALIDATION_FAILED,
        "The request is invalid: see errors",
        request,
        errors,
        Map.of());
  }

  private static String leaf(String path) {
    return path.substring(path.lastIndexOf('.') + 1);
  }
}
