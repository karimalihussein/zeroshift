package io.zeroshift.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.zeroshift.domain.InvalidAction;
import io.zeroshift.domain.OrderEdit;
import io.zeroshift.eventlab.LabServices;
import io.zeroshift.platform.web.ApiExceptionHandler;
import io.zeroshift.platform.web.RequestIdFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.resource.NoResourceFoundException;

class LabApiErrorsTest {
  @RestController
  static class Probe {
    @PostMapping("/invalid")
    void invalid() {
      throw new InvalidAction("Cutover needs a validated copy");
    }

    @PostMapping("/edit")
    OrderEdit edit(@RequestBody OrderEdit edit) {
      return edit;
    }

    @PostMapping("/upstream")
    void upstream() {
      throw new LabServices.ActionFailed("POST order-service/orders → HTTP 500");
    }

    @GetMapping("/missing")
    void missing() throws NoResourceFoundException {
      throw new NoResourceFoundException(HttpMethod.GET, "/missing", "missing");
    }

    @GetMapping("/boom")
    void boom() {
      throw new IllegalStateException("jdbc url and password");
    }
  }

  MockMvc mvc;

  @BeforeEach
  void setUp() {
    mvc =
        MockMvcBuilders.standaloneSetup(new Probe())
            .setControllerAdvice(new LabApiErrors(), new ApiExceptionHandler())
            .addFilters(new RequestIdFilter())
            .build();
  }

  @Test
  void anInvalidActionIsAConflictWithItsReason() throws Exception {
    mvc.perform(post("/invalid"))
        .andExpect(status().isConflict())
        .andExpect(content().contentType(MediaType.APPLICATION_PROBLEM_JSON))
        .andExpect(jsonPath("$.code").value("INVALID_ACTION"))
        .andExpect(jsonPath("$.detail").value("Cutover needs a validated copy"))
        .andExpect(jsonPath("$.requestId").exists());
  }

  @Test
  void aBodyTheDomainRefusesKeepsTheDomainsReason() throws Exception {
    mvc.perform(
            post("/edit")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"customerName\":\"\",\"amount\":1,\"status\":\"NEW\"}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"))
        .andExpect(jsonPath("$.detail").value("Customer name must contain 1–180 characters"));
    mvc.perform(post("/edit").contentType(MediaType.APPLICATION_JSON).content("{nope"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("MALFORMED_REQUEST"));
  }

  @Test
  void aFailedServiceCallIsABadGateway() throws Exception {
    mvc.perform(post("/upstream"))
        .andExpect(status().isBadGateway())
        .andExpect(jsonPath("$.code").value("BAD_GATEWAY"));
  }

  @Test
  void anUnknownRouteStaysNotFoundAndAnUnexpectedFailureHidesItsCause() throws Exception {
    mvc.perform(get("/missing"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.code").value("NOT_FOUND"));
    var body =
        mvc.perform(get("/boom"))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.code").value("INTERNAL_ERROR"))
            .andReturn()
            .getResponse()
            .getContentAsString();
    assertThat(body).doesNotContain("password");
  }
}
