package io.zeroshift.kafkalab;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;

/**
 * The lab's topics. Every one is replicated; the scenario topics are recreated by each run with an
 * explicit replica assignment, so a run knows which node leads and which follows.
 */
public final class LabTopics {
  /** Continuous traffic during broker failures: 3 partitions, 3 replicas, 2 must be in sync. */
  public static final String REPLICATED = "lab.replicated";

  /** Single writes probing acks and min.insync.replicas. */
  public static final String DURABILITY = "lab.durability";

  /** acks=1 vs acks=all against a crashing leader: 2 replicas, assignment chosen per run. */
  public static final String ACKS = "lab.acks";

  /** Producer retries with and without idempotence. */
  public static final String RETRIES = "lab.retries";

  /** The last in-sync replica dies: offline partition, clean wait vs unclean election. */
  public static final String UNCLEAN = "lab.unclean";

  /** Delivery semantics: a consume-transform-produce worker reads IN and writes OUT. */
  public static final String DELIVERY_IN = "lab.delivery.in";

  public static final String DELIVERY_OUT = "lab.delivery.out";

  public static final String PREFIX = "lab.";

  static final String MIN_ISR = "min.insync.replicas";
  static final String UNCLEAN_ELECTION = "unclean.leader.election.enable";

  private LabTopics() {}

  /** The long-lived topics, created on setup if missing. */
  static List<NewTopic> standing() {
    return List.of(
        replicated(REPLICATED, 3, 2), replicated(DURABILITY, 1, 2), replicated(RETRIES, 1, 2));
  }

  static NewTopic replicated(String name, int partitions, int minIsr) {
    return new NewTopic(name, partitions, (short) 3)
        .configs(Map.of(MIN_ISR, String.valueOf(minIsr), "retention.ms", "3600000"));
  }

  /** One partition on exactly these nodes, the first being the preferred leader. */
  static NewTopic pinned(String name, int minIsr, Integer... replicas) {
    return new NewTopic(name, Map.of(0, List.of(replicas)))
        .configs(Map.of(MIN_ISR, String.valueOf(minIsr), UNCLEAN_ELECTION, "false"));
  }

  /**
   * What the lab actually creates. Standing topics and the delivery pair come from {@link
   * #replicated}; acks and unclean come from {@link #pinned} with two replicas and one partition.
   */
  public static List<Declared> declared() {
    var notes =
        Map.of(
            REPLICATED, "Continuous traffic while brokers fail. Created on setup if missing.",
            DURABILITY, "Single writes that probe acks and min.insync.replicas.",
            RETRIES, "Producer retries with and without idempotence.");
    var topics = new java.util.ArrayList<Declared>();
    for (var topic : standing()) topics.add(from(topic, notes.get(topic.name())));
    topics.add(
        from(
            replicated(DELIVERY_IN, 3, 2),
            "Input of the consume-transform-produce worker. Recreated each delivery run."));
    topics.add(
        from(
            replicated(DELIVERY_OUT, 3, 2),
            "Output of the delivery worker. Recreated each delivery run."));
    topics.add(
        new Declared(
            ACKS,
            1,
            (short) 2,
            1,
            null,
            "Recreated each acks run by pinned(acks, minIsr=1, leader, follower). Unclean election is off. Retention is the broker default."));
    topics.add(
        new Declared(
            UNCLEAN,
            1,
            (short) 2,
            1,
            null,
            "Recreated each unclean-election run by pinned(unclean, minIsr=1, leader, follower). unclean.leader.election.enable starts false."));
    return List.copyOf(topics);
  }

  private static Declared from(NewTopic topic, String notes) {
    return new Declared(
        topic.name(),
        topic.numPartitions(),
        topic.replicationFactor(),
        Integer.parseInt(topic.configs().get(MIN_ISR)),
        topic.configs().get("retention.ms"),
        notes);
  }

  /** One lab topic as it is created, for the developer portal and for tests. */
  public record Declared(
      String name,
      Integer partitions,
      Short replicationFactor,
      int minInSyncReplicas,
      String retentionMs,
      String notes) {}
}
