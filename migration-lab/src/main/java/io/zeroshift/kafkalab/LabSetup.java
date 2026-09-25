package io.zeroshift.kafkalab;

import io.zeroshift.platform.web.ApiException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/** Getting the lab cluster into a known state: every node up, the standing topics present. */
@Component
public class LabSetup {
  private final LabNodes nodes;
  private final LabKafka kafka;
  private final ClusterObserver observer;

  public LabSetup(LabNodes nodes, LabKafka kafka, ClusterObserver observer) {
    this.nodes = nodes;
    this.kafka = kafka;
    this.observer = observer;
  }

  public void ensureStanding() {
    requireHealthy();
    var existing = kafka.topics();
    for (var topic : LabTopics.standing())
      if (!existing.contains(topic.name())) kafka.create(topic);
  }

  /**
   * A scenario starts from a whole cluster: every container running, every broker registered and a
   * quorum leader. Otherwise it would demonstrate the leftovers of the last one.
   */
  public ClusterObserver.Snapshot requireHealthy() {
    var s = observer.observe();
    var down =
        s.nodes().stream()
            .filter(n -> !n.state().equals("running") || !n.registeredBroker())
            .map(
                n ->
                    "node "
                        + n.id()
                        + " ("
                        + n.state()
                        + (n.registeredBroker() ? "" : ", not registered")
                        + ")")
            .toList();
    if (s.nodes().size() < 3
        || !down.isEmpty()
        || s.quorum() == null
        || s.quorum().leaderId() == null)
      throw new ApiException(
          HttpStatus.CONFLICT,
          "CLUSTER_DEGRADED",
          s.nodes().isEmpty()
              ? String.join("; ", s.problems())
              : "This scenario needs all 3 nodes up with a quorum leader; recover first"
                  + (down.isEmpty() ? "" : ": " + String.join(", ", down)));
    return s;
  }

  /**
   * Picks roles for a two-replica scenario: {@code [leader, follower]} from the nodes that are not
   * the controller quorum leader. Failing those two never costs the controller quorum its majority
   * (the third node and whichever of them is up still form one), so the scenario shows exactly one
   * thing: what happens to the partition.
   */
  public List<Integer> pickPair(ClusterObserver.Snapshot s) {
    var others =
        s.nodes().stream()
            .map(ClusterObserver.NodeView::id)
            .filter(id -> !id.equals(s.quorum().leaderId()))
            .collect(Collectors.toCollection(ArrayList::new));
    java.util.Collections.shuffle(others);
    return others.subList(0, 2);
  }

  /** Every node started and unfrozen, then waits until all three are registered again. */
  public ClusterObserver.Snapshot recoverAll() {
    for (var n : nodes.nodes())
      switch (n.state()) {
        case "paused" -> nodes.act(n.id(), LabNodes.Action.UNPAUSE);
        case "running", "restarting" -> {}
        default -> nodes.act(n.id(), LabNodes.Action.START);
      }
    long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
    while (System.nanoTime() < deadline) {
      var s = observer.observe();
      if (s.nodes().size() == 3
          && s.nodes().stream().allMatch(n -> n.state().equals("running") && n.registeredBroker())
          && s.quorum() != null
          && s.quorum().leaderId() != null) return s;
      LabKafka.sleep(1000);
    }
    throw new ApiException(
        HttpStatus.SERVICE_UNAVAILABLE,
        "CLUSTER_NOT_RECOVERED",
        "The lab nodes did not all come back in 90 s");
  }

  /** Moves leadership back to each partition's first replica, as auto-rebalance would later. */
  public int electPreferred() {
    var partitions = new java.util.HashSet<TopicPartition>();
    var s = observer.observe();
    for (var t : s.topics())
      for (var p : t.partitions())
        if (!p.replicas().isEmpty() && !p.replicas().getFirst().equals(p.leader()))
          partitions.add(new TopicPartition(t.name(), p.partition()));
    if (partitions.isEmpty()) return 0;
    var result = kafka.admin().electLeaders(ElectionType.PREFERRED, partitions).partitions();
    int moved = 0;
    for (var e : kafka.await(result).entrySet()) if (e.getValue().isEmpty()) moved++;
    return moved;
  }

  /** Back to a clean lab: every node up, scenario topics deleted, the standing topics present. */
  public ClusterObserver.Snapshot reset() {
    recoverAll();
    var scenarioTopics =
        kafka.topics().stream()
            .filter(t -> t.startsWith(LabTopics.PREFIX))
            .filter(t -> LabTopics.standing().stream().noneMatch(st -> st.name().equals(t)))
            .toList();
    if (!scenarioTopics.isEmpty()) kafka.await(kafka.admin().deleteTopics(scenarioTopics).all());
    ensureStanding();
    // A scenario may have changed a standing topic's settings (min.insync.replicas): restore them.
    var restore =
        new java.util.HashMap<
            org.apache.kafka.common.config.ConfigResource,
            java.util.Collection<org.apache.kafka.clients.admin.AlterConfigOp>>();
    for (var topic : LabTopics.standing())
      restore.put(
          new org.apache.kafka.common.config.ConfigResource(
              org.apache.kafka.common.config.ConfigResource.Type.TOPIC, topic.name()),
          topic.configs().entrySet().stream()
              .map(
                  e ->
                      new org.apache.kafka.clients.admin.AlterConfigOp(
                          new org.apache.kafka.clients.admin.ConfigEntry(e.getKey(), e.getValue()),
                          org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET))
              .toList());
    kafka.await(kafka.admin().incrementalAlterConfigs(restore).all());
    return observer.observe();
  }
}
