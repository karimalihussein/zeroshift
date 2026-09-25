package io.zeroshift.kafkalab;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Where the Phase 2 Kafka lab lives: a separate 3-node KRaft cluster (Compose profile kafka-lab)
 * and the Docker API proxy that lets the control plane kill, stop, pause and restart its nodes.
 * Blank when not configured; the lab then says how to start it instead of failing.
 *
 * @param cluster the value of the {@code zeroshift.kafka-lab.cluster} label on this cluster's
 *     containers, so the control plane only ever touches its own lab nodes
 */
@ConfigurationProperties("kafka-lab")
public record KafkaLabSettings(String bootstrap, String dockerUrl, String cluster) {
  public boolean configured() {
    return bootstrap != null && !bootstrap.isBlank() && dockerUrl != null && !dockerUrl.isBlank();
  }
}
