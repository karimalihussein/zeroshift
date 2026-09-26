package io.zeroshift.failures;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.Test;

class SecurityDecisionTest {
  static SecurityDecision of(Throwable t) {
    return SecurityDecision.from("secure", "User:x", "WRITE", "topic t", t, true);
  }

  @Test
  void theBrokersAnswerDecidesTheStage() {
    var authz =
        of(
            new ExecutionException(
                new TopicAuthorizationException("Not authorized to access topics: [t]")));
    assertThat(authz.decision()).isEqualTo("DENIED");
    assertThat(authz.stage()).isEqualTo("authorization");
    assertThat(authz.error()).isEqualTo("TopicAuthorizationException");

    assertThat(of(new GroupAuthorizationException("group")).stage()).isEqualTo("authorization");

    var authn =
        of(
            new ExecutionException(
                new SaslAuthenticationException(
                    "Authentication failed: Invalid username or password")));
    assertThat(authn.stage()).isEqualTo("authentication");

    var timeout =
        new ExecutionException(
            new TimeoutException("Topic t not present in metadata after 5000 ms."));
    var plaintext =
        SecurityDecision.from("secure", "no credentials", "WRITE", "topic t", timeout, false);
    assertThat(plaintext.decision()).isEqualTo("DENIED");
    assertThat(plaintext.stage()).isEqualTo("connection");
  }

  @Test
  void anAuthenticatedClientTimingOutGotNoDecision() {
    var timeout =
        new ExecutionException(new TimeoutException("Topic t not present in metadata"));
    assertThat(of(timeout).decision()).isEqualTo("FAILED");
  }

  @Test
  void anythingElseIsAFailureNotADecision() {
    var other = of(new IllegalStateException("boom"));
    assertThat(other.decision()).isEqualTo("FAILED");
    assertThat(other.stage()).isEqualTo("error");
  }

  @Test
  void allowedCarriesTheOffset() {
    var ok =
        SecurityDecision.allowed(
            "open", "anonymous", "WRITE", "topic t", 42L, "partition 0, offset 42");
    assertThat(ok.decision()).isEqualTo("ALLOWED");
    assertThat(ok.offset()).isEqualTo(42L);
  }
}
