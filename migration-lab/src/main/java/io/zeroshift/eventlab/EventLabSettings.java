package io.zeroshift.eventlab;

import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Where the event-driven lab's infrastructure lives. The migration lab works without it: when the
 * stack is not running the control plane reports it unreachable instead of failing.
 */
@ConfigurationProperties("event-lab")
public record EventLabSettings(
    boolean enabled,
    String kafkaBootstrap,
    String connectUrl,
    Map<String, String> services,
    String grafanaUrl) {}
