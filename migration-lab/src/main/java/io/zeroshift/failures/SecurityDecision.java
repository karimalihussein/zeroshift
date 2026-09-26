package io.zeroshift.failures;

import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;

/**
 * What a broker decided about one client operation, read from what the broker answered: an offset
 * (allowed), an authentication failure, an authorization failure, or a connection it would not
 * serve. Never inferred by the lab.
 */
record SecurityDecision(
    String broker,
    String principal,
    String operation,
    String resource,
    String decision,
    String stage,
    String error,
    String detail,
    Long offset) {

  static SecurityDecision allowed(
      String broker,
      String principal,
      String operation,
      String resource,
      Long offset,
      String detail) {
    return new SecurityDecision(
        broker, principal, operation, resource, "ALLOWED", null, null, detail, offset);
  }

  /**
   * @param authenticated whether the client presented credentials. Only a client without them can
   *     be refused by the connection itself (the broker serves nothing before SASL); for any other
   *     client a timeout is not a decision, only a slow or unreachable broker, and is FAILED.
   */
  static SecurityDecision from(
      String broker,
      String principal,
      String operation,
      String resource,
      Throwable failure,
      boolean authenticated) {
    var cause = root(failure);
    String stage;
    if (cause instanceof SaslAuthenticationException) stage = "authentication";
    else if (cause instanceof TopicAuthorizationException
        || cause instanceof GroupAuthorizationException
        || cause instanceof AuthorizationException) stage = "authorization";
    else if (cause instanceof org.apache.kafka.common.errors.TimeoutException && !authenticated) stage = "connection";
    else stage = "error";
    var detail = cause.getMessage() == null ? "" : cause.getMessage();
    if (stage.equals("connection"))
      detail =
          "the broker served nothing to a client without SASL credentials; it gave up after: "
              + detail;
    return new SecurityDecision(
        broker,
        principal,
        operation,
        resource,
        stage.equals("error") ? "FAILED" : "DENIED",
        stage,
        cause.getClass().getSimpleName(),
        detail,
        null);
  }

  static Throwable root(Throwable t) {
    var cause = t;
    while (cause.getCause() != null
        && cause.getCause() != cause
        && (cause instanceof java.util.concurrent.ExecutionException
            || cause instanceof org.apache.kafka.common.KafkaException
                && !(cause instanceof org.apache.kafka.common.errors.ApiException)))
      cause = cause.getCause();
    return cause;
  }
}
