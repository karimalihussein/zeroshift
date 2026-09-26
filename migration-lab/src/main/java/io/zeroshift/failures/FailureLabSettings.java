package io.zeroshift.failures;

import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Where the Phase 5 failure lab's own infrastructure lives (Compose profile failure-lab). Blank
 * values mean it is not running: the lab then says how to start it and the rest of the control
 * plane is unaffected.
 *
 * @param postgresUrl the isolated PostgreSQL (prepared transactions enabled), any database on it
 * @param secureKafka the SASL + ACL broker's bootstrap address
 * @param passwords SASL/PLAIN passwords per principal: admin, payments, fulfilment, checkout
 */
@ConfigurationProperties("failure-lab")
public record FailureLabSettings(
    String postgresUrl,
    String postgresUser,
    String postgresPassword,
    String secureKafka,
    Map<String, String> passwords) {

  public boolean postgresConfigured() {
    return postgresUrl != null && !postgresUrl.isBlank();
  }

  public boolean secureKafkaConfigured() {
    return secureKafka != null && !secureKafka.isBlank();
  }

  public String password(String principal) {
    return passwords == null ? null : passwords.get(principal);
  }
}
