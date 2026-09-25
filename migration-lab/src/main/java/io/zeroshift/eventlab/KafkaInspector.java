package io.zeroshift.eventlab;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Component;

/**
 * Reads the broker's own view through the AdminClient: partitions with their earliest and latest
 * offsets, and every consumer group with its members, assignments, committed offsets and lag.
 */
@Component
public class KafkaInspector implements AutoCloseable {
  public record PartitionView(int partition, long earliest, long latest) {}

  public record TopicView(String name, String retentionMs, List<PartitionView> partitions) {}

  public record Member(String clientId, String host, List<String> assignments) {}

  public record GroupPartition(
      String topic, int partition, Long committed, long latest, Long lag) {}

  public record GroupView(
      String groupId,
      String state,
      List<Member> members,
      List<GroupPartition> partitions,
      long totalLag) {}

  private final EventLabSettings settings;
  private Admin admin;

  public KafkaInspector(EventLabSettings settings) {
    this.settings = settings;
  }

  private synchronized Admin admin() {
    if (admin == null)
      admin =
          Admin.create(
              Map.of(
                  AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, settings.kafkaBootstrap(),
                  AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000,
                  AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 4000));
    return admin;
  }

  public List<TopicView> topics() throws Exception {
    var names =
        admin().listTopics().names().get(4, TimeUnit.SECONDS).stream()
            .filter(t -> EventTap.TOPICS.matcher(t).matches())
            .sorted()
            .toList();
    var described = admin().describeTopics(names).allTopicNames().get(4, TimeUnit.SECONDS);
    var partitions =
        described.values().stream()
            .flatMap(
                d -> d.partitions().stream().map(p -> new TopicPartition(d.name(), p.partition())))
            .toList();
    var earliest = offsets(partitions, OffsetSpec.earliest());
    var latest = offsets(partitions, OffsetSpec.latest());
    var configs =
        admin()
            .describeConfigs(
                names.stream().map(n -> new ConfigResource(ConfigResource.Type.TOPIC, n)).toList())
            .all()
            .get(4, TimeUnit.SECONDS);
    return names.stream()
        .map(
            name ->
                new TopicView(
                    name,
                    configs
                        .get(new ConfigResource(ConfigResource.Type.TOPIC, name))
                        .get("retention.ms")
                        .value(),
                    described.get(name).partitions().stream()
                        .map(
                            p -> {
                              var tp = new TopicPartition(name, p.partition());
                              return new PartitionView(
                                  p.partition(), earliest.get(tp), latest.get(tp));
                            })
                        .toList()))
        .toList();
  }

  public List<GroupView> groups() throws Exception {
    var ids =
        admin().listConsumerGroups().all().get(4, TimeUnit.SECONDS).stream()
            .map(ConsumerGroupListing::groupId)
            .filter(id -> !id.startsWith("test-") && !id.equals("zeroshift-connect"))
            .sorted()
            .toList();
    var descriptions = admin().describeConsumerGroups(ids).all().get(4, TimeUnit.SECONDS);
    var result = new ArrayList<GroupView>();
    for (var id : ids) {
      var committed =
          admin()
              .listConsumerGroupOffsets(id)
              .partitionsToOffsetAndMetadata()
              .get(4, TimeUnit.SECONDS);
      var assigned =
          descriptions.get(id).members().stream()
              .flatMap(m -> m.assignment().topicPartitions().stream())
              .collect(Collectors.toSet());
      var partitions = new TreeSet<TopicPartition>(Comparator.comparing(TopicPartition::toString));
      partitions.addAll(committed.keySet());
      partitions.addAll(assigned);
      var latest = offsets(List.copyOf(partitions), OffsetSpec.latest());
      var earliest = offsets(List.copyOf(partitions), OffsetSpec.earliest());
      var views =
          partitions.stream()
              .map(
                  tp -> {
                    OffsetAndMetadata c = committed.get(tp);
                    long end = latest.getOrDefault(tp, 0L);
                    // Never committed (a new group, a recreated topic, an added partition): every
                    // lab consumer starts from the earliest offset, so all of it is still to read.
                    long position = c == null ? earliest.getOrDefault(tp, 0L) : c.offset();
                    return new GroupPartition(
                        tp.topic(),
                        tp.partition(),
                        c == null ? null : c.offset(),
                        end,
                        Math.max(0, end - position));
                  })
              .toList();
      var d = descriptions.get(id);
      result.add(
          new GroupView(
              id,
              d.groupState().name(),
              d.members().stream()
                  .map(
                      m ->
                          new Member(
                              m.clientId(),
                              m.host(),
                              m.assignment().topicPartitions().stream()
                                  .map(TopicPartition::toString)
                                  .sorted()
                                  .toList()))
                  .sorted(Comparator.comparing(Member::clientId))
                  .toList(),
              views,
              views.stream().mapToLong(v -> v.lag() == null ? 0 : v.lag()).sum()));
    }
    return result;
  }

  /** Moves a group's committed offset. Kafka refuses while the group still has members. */
  public void moveOffset(String group, String topic, int partition, long offset) throws Exception {
    admin()
        .alterConsumerGroupOffsets(
            group, Map.of(new TopicPartition(topic, partition), new OffsetAndMetadata(offset)))
        .all()
        .get(6, TimeUnit.SECONDS);
  }

  /** Waits until every member has left the group (its consumers were stopped). */
  public void awaitEmpty(String group) throws Exception {
    for (int i = 0; i < 40; i++) {
      var d =
          admin().describeConsumerGroups(List.of(group)).all().get(4, TimeUnit.SECONDS).get(group);
      if (d.members().isEmpty()) return;
      Thread.sleep(250);
    }
    throw new IllegalStateException(group + " still has members after its consumers were stopped");
  }

  private Map<TopicPartition, Long> offsets(List<TopicPartition> partitions, OffsetSpec spec)
      throws Exception {
    if (partitions.isEmpty()) return Map.of();
    return admin()
        .listOffsets(partitions.stream().collect(Collectors.toMap(tp -> tp, tp -> spec)))
        .all()
        .get(4, TimeUnit.SECONDS)
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
  }

  @Override
  public synchronized void close() {
    if (admin != null) admin.close();
  }
}
