package io.zeroshift.platform.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.web.bind.annotation.*;

class ApiConventionsTest {
  record Greeting(@NotBlank String name) {}

  @RestController
  static class Probe {
    @PostMapping("/greetings")
    Greeting greet(@Valid @RequestBody Greeting greeting) {
      return greeting;
    }

    @PostMapping("/keyed")
    Greeting keyed(
        @Valid @RequestBody Greeting greeting,
        @RequestHeader(name = "Idempotency-Key", required = false) @Size(max = 5) String key) {
      return greeting;
    }

    @GetMapping("/items")
    ApiResponse<List<Integer>> items(@RequestParam(defaultValue = "2") @Min(1) @Max(50) int limit) {
      return ApiResponse.page(List.of(1, 2, 3), limit);
    }

    @GetMapping("/conflict")
    void conflict() {
      throw new ApiException(
          HttpStatus.CONFLICT, "THING_BUSY", "The thing is busy", Map.of("retryAfterMs", 250));
    }

    @GetMapping("/boom")
    void boom() {
      throw new IllegalStateException("internal detail that must not leak");
    }
  }

  MockMvc mvc;

  @BeforeEach
  void setUp() {
    var validator = new LocalValidatorFactoryBean();
    validator.afterPropertiesSet();
    mvc =
        MockMvcBuilders.standaloneSetup(new Probe())
            .setControllerAdvice(new ApiExceptionHandler())
            .setValidator(validator)
            .addFilters(new RequestIdFilter())
            .build();
  }

  @Test
  void anInvalidBodyIsA400WithFieldErrorsAndTheRequestId() throws Exception {
    mvc.perform(
            post("/greetings")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\" \"}")
                .header(ApiHeaders.REQUEST_ID, "req-42"))
        .andExpect(status().isBadRequest())
        .andExpect(header().string(ApiHeaders.REQUEST_ID, "req-42"))
        .andExpect(jsonPath("$.code").value(ErrorCodes.VALIDATION_FAILED))
        .andExpect(jsonPath("$.status").value(400))
        .andExpect(jsonPath("$.requestId").value("req-42"))
        .andExpect(jsonPath("$.instance").value("/greetings"))
        .andExpect(jsonPath("$.errors[0].field").value("name"));
  }

  @Test
  void anInvalidParameterIsA400NamingTheParameter() throws Exception {
    mvc.perform(get("/items?limit=0"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value(ErrorCodes.VALIDATION_FAILED))
        .andExpect(jsonPath("$.errors[0].field").value("limit"));
    mvc.perform(get("/items?limit=many"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value(ErrorCodes.INVALID_PARAMETER));
  }

  @Test
  void aBodyValidatedWithConstrainedParametersStillNamesItsFields() throws Exception {
    mvc.perform(
            post("/keyed")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\" \"}")
                .header(ApiHeaders.IDEMPOTENCY_KEY, "far-too-long"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value(ErrorCodes.VALIDATION_FAILED))
        .andExpect(
            jsonPath("$.errors[*].field")
                .value(org.hamcrest.Matchers.containsInAnyOrder("name", "Idempotency-Key")));
  }

  @Test
  void malformedJsonIsA400() throws Exception {
    mvc.perform(post("/greetings").contentType(MediaType.APPLICATION_JSON).content("{not json"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value(ErrorCodes.MALFORMED_REQUEST));
  }

  @Test
  void anApiExceptionKeepsItsStatusCodeAndContext() throws Exception {
    mvc.perform(get("/conflict"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.code").value("THING_BUSY"))
        .andExpect(jsonPath("$.detail").value("The thing is busy"))
        .andExpect(jsonPath("$.context.retryAfterMs").value(250))
        .andExpect(jsonPath("$.type").value("urn:zeroshift:error:thing-busy"));
  }

  @Test
  void anUnexpectedErrorIsA500ThatHidesItsCauseButNamesTheRequest() throws Exception {
    var body =
        mvc.perform(get("/boom"))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.code").value(ErrorCodes.INTERNAL_ERROR))
            .andExpect(jsonPath("$.requestId").exists())
            .andReturn()
            .getResponse()
            .getContentAsString();
    assertThat(body).doesNotContain("internal detail");
  }

  @Test
  void listsCarryTheirMetaAndARequestIdIsGeneratedWhenNoneIsGiven() throws Exception {
    mvc.perform(get("/items?limit=2"))
        .andExpect(status().isOk())
        .andExpect(header().exists(ApiHeaders.REQUEST_ID))
        .andExpect(jsonPath("$.data.length()").value(2))
        .andExpect(jsonPath("$.meta.count").value(2))
        .andExpect(jsonPath("$.meta.limit").value(2))
        .andExpect(jsonPath("$.meta.hasMore").value(true));
    // A malformed caller id is replaced, not echoed.
    mvc.perform(get("/items").header(ApiHeaders.REQUEST_ID, "bad id with spaces"))
        .andExpect(
            header()
                .string(ApiHeaders.REQUEST_ID, org.hamcrest.Matchers.not("bad id with spaces")));
  }
}
