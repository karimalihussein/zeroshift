package io.zeroshift.racelab.web;

import io.zeroshift.platform.web.ApiError;
import io.zeroshift.platform.web.ApiErrors;
import io.zeroshift.platform.web.ApiExceptionHandler;
import io.zeroshift.racelab.domain.RaceLabErrors;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** The race lab's refusals in the shared error format, with stable codes. */
@RestControllerAdvice(assignableTypes = RaceLabController.class)
@Order(ApiExceptionHandler.SERVICE_ADVICE)
public class RaceLabApiErrors {
  public static final String EXPERIMENT_NOT_FOUND = "EXPERIMENT_NOT_FOUND";
  public static final String RUN_NOT_FOUND = "RUN_NOT_FOUND";
  public static final String INVALID_RUN_CONFIG = "INVALID_RUN_CONFIG";
  public static final String RACE_LAB_BUSY = "RACE_LAB_BUSY";
  public static final String RUN_ALREADY_STARTED = "RUN_ALREADY_STARTED";

  @ExceptionHandler(RaceLabErrors.ExperimentNotFound.class)
  ResponseEntity<ApiError> experiment(RaceLabErrors.ExperimentNotFound e, HttpServletRequest request) {
    return ApiErrors.response(HttpStatus.NOT_FOUND, EXPERIMENT_NOT_FOUND, e.getMessage(), request);
  }

  @ExceptionHandler(RaceLabErrors.RunNotFound.class)
  ResponseEntity<ApiError> run(RaceLabErrors.RunNotFound e, HttpServletRequest request) {
    return ApiErrors.response(HttpStatus.NOT_FOUND, RUN_NOT_FOUND, e.getMessage(), request);
  }

  @ExceptionHandler(RaceLabErrors.InvalidConfig.class)
  ResponseEntity<ApiError> config(RaceLabErrors.InvalidConfig e, HttpServletRequest request) {
    return ApiErrors.response(HttpStatus.BAD_REQUEST, INVALID_RUN_CONFIG, e.getMessage(), request);
  }

  @ExceptionHandler(RaceLabErrors.LabBusy.class)
  ResponseEntity<ApiError> busy(RaceLabErrors.LabBusy e, HttpServletRequest request) {
    return ApiErrors.response(
        HttpStatus.CONFLICT,
        RACE_LAB_BUSY,
        e.getMessage(),
        request,
        List.of(),
        Map.of("runningRunId", e.runningId()));
  }

  @ExceptionHandler(RaceLabErrors.RunAlreadyStarted.class)
  ResponseEntity<ApiError> started(RaceLabErrors.RunAlreadyStarted e, HttpServletRequest request) {
    return ApiErrors.response(HttpStatus.CONFLICT, RUN_ALREADY_STARTED, e.getMessage(), request);
  }
}
