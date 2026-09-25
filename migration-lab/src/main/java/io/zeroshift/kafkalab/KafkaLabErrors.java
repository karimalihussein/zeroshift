package io.zeroshift.kafkalab;

import io.zeroshift.platform.web.ApiError;
import io.zeroshift.platform.web.ApiErrors;
import io.zeroshift.platform.web.ApiExceptionHandler;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.kafka.common.KafkaException;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Failures are what this lab is about, so Kafka's own words are the useful answer: a broker that
 * timed out or refused is a 503 with Kafka's message, a scenario that could not complete a 409.
 */
@RestControllerAdvice(assignableTypes = KafkaLabController.class)
@Order(ApiExceptionHandler.SERVICE_ADVICE)
public class KafkaLabErrors {
  @ExceptionHandler(KafkaException.class)
  ResponseEntity<ApiError> kafka(KafkaException e, HttpServletRequest request) {
    return ApiErrors.response(
        HttpStatus.SERVICE_UNAVAILABLE, "KAFKA_ERROR", ClusterObserver.rootMessage(e), request);
  }

  @ExceptionHandler(ScenarioFailed.class)
  ResponseEntity<ApiError> scenario(ScenarioFailed e, HttpServletRequest request) {
    return ApiErrors.response(HttpStatus.CONFLICT, "SCENARIO_FAILED", e.getMessage(), request);
  }

  /**
   * A scenario step that did not happen on the real cluster (no leader in time, a write refused).
   */
  public static final class ScenarioFailed extends RuntimeException {
    public ScenarioFailed(String message) {
      super(message);
    }

    public ScenarioFailed(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
