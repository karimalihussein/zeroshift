package io.zeroshift.web;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.web.servlet.resource.NoResourceFoundException;

class ApiExceptionHandlerTest {
  private final ApiExceptionHandler handler = new ApiExceptionHandler();

  @Test
  void unknownRouteIsNotFoundRatherThanADatabaseFailure() {
    var response =
        handler.failure(new NoResourceFoundException(HttpMethod.POST, "/api/reset", "api/reset"));
    assertThat(response.getStatusCode().value()).isEqualTo(404);
  }

  @Test
  void unexpectedFailureStaysInternalServerError() {
    var response = handler.failure(new IllegalStateException("boom"));
    assertThat(response.getStatusCode().value()).isEqualTo(500);
  }
}
