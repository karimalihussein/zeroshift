package io.zeroshift.kafkalab;

import io.zeroshift.platform.web.ApiException;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.jar.JarFile;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Delivery semantics, measured. Each run gets fresh input and output topics, writes records 1..N to
 * the input, runs a {@link DeliveryWorker} JVM that may crash at record k, restarts it, and then
 * reads the output twice, read_committed and read_uncommitted, counting every record: missing (a
 * gap), once, or more than once (a duplicate).
 */
@Component
public class DeliveryLab {
  static final int RECORDS = 30;
  static final int CRASH_AT = 12;

  public record Worker(int number, int exitCode, boolean crashed, long ms, List<String> events) {}

  /**
   * One reading of the output topic.
   *
   * @param copies how many times each input record 1..N appears, index 0 = record 1
   */
  public record Outcome(
      String isolation,
      int total,
      List<Integer> copies,
      List<Integer> missing,
      List<Integer> duplicated) {}

  public record DeliveryRun(
      String mode,
      boolean crash,
      int records,
      Integer crashAt,
      String run,
      List<Worker> workers,
      Outcome committed,
      Outcome uncommitted,
      List<ClusterObserver.GroupPartition> committedOffsets,
      String verdict) {}

  private final LabKafka kafka;
  private final LabSetup setup;
  private final KafkaLabRuns runs;
  private final ReentrantLock busy = new ReentrantLock();

  public DeliveryLab(LabKafka kafka, LabSetup setup, KafkaLabRuns runs) {
    this.kafka = kafka;
    this.setup = setup;
    this.runs = runs;
  }

  public KafkaLabRuns.Run run(DeliveryWorker.Mode mode, boolean crash) {
    if (!busy.tryLock())
      throw new ApiException(
          HttpStatus.CONFLICT, "SCENARIO_RUNNING", "A delivery run is already in progress");
    try {
      setup.requireHealthy();
      var run = UUID.randomUUID().toString().substring(0, 8);
      forgetOldGroups();
      kafka.recreate(topic(LabTopics.DELIVERY_IN));
      kafka.recreate(topic(LabTopics.DELIVERY_OUT));
      try (var producer =
          kafka.producer(
              Map.of(
                  ProducerConfig.ACKS_CONFIG,
                  "all",
                  ProducerConfig.CLIENT_ID_CONFIG,
                  "lab-delivery-input"))) {
        for (int seq = 1; seq <= RECORDS; seq++)
          producer.send(
              new ProducerRecord<>(LabTopics.DELIVERY_IN, "order-" + seq, run + ":" + seq));
        producer.flush();
      }
      var workers = new ArrayList<Worker>();
      var first = launch(mode, run, crash ? CRASH_AT : 0, 1);
      workers.add(first);
      if (first.crashed()) workers.add(launch(mode, run, 0, 2));
      var committed = outcome(run, IsolationLevel.READ_COMMITTED);
      var uncommitted = outcome(run, IsolationLevel.READ_UNCOMMITTED);
      var offsets = groupOffsets(run);
      var verdict = verdict(mode, crash, committed, uncommitted);
      return runs.record(
          KafkaLabRuns.DELIVERY,
          mode + (crash ? " + crash" : ""),
          verdict,
          new DeliveryRun(
              mode.name(),
              crash,
              RECORDS,
              crash ? CRASH_AT : null,
              run,
              workers,
              committed,
              uncommitted,
              offsets,
              verdict));
    } finally {
      busy.unlock();
    }
  }

  static String verdict(
      DeliveryWorker.Mode mode, boolean crash, Outcome committed, Outcome uncommitted) {
    var c =
        committed.missing().isEmpty() && committed.duplicated().isEmpty()
            ? "every record exactly once"
            : (committed.missing().isEmpty()
                    ? ""
                    : committed.missing().size() + " missing " + committed.missing())
                + (!committed.missing().isEmpty() && !committed.duplicated().isEmpty() ? ", " : "")
                + (committed.duplicated().isEmpty()
                    ? ""
                    : committed.duplicated().size() + " duplicated " + committed.duplicated());
    var extra =
        mode == DeliveryWorker.Mode.EXACTLY_ONCE && !uncommitted.duplicated().isEmpty()
            ? "; a read_uncommitted consumer still sees the aborted copies of "
                + uncommitted.duplicated()
            : "";
    return mode
        + (crash ? " with a crash at record " + CRASH_AT : " without a crash")
        + ": "
        + c
        + extra;
  }

  private Outcome outcome(String run, IsolationLevel isolation) {
    var copies = new TreeMap<Integer, Integer>();
    int total = 0;
    for (var r : kafka.readAll(LabTopics.DELIVERY_OUT, isolation)) {
      var parts = r.value().split(":");
      if (parts.length < 2 || !parts[0].equals(run)) continue;
      copies.merge(Integer.parseInt(parts[1]), 1, Integer::sum);
      total++;
    }
    var list = new ArrayList<Integer>();
    var missing = new ArrayList<Integer>();
    var duplicated = new ArrayList<Integer>();
    for (int seq = 1; seq <= RECORDS; seq++) {
      int n = copies.getOrDefault(seq, 0);
      list.add(n);
      if (n == 0) missing.add(seq);
      if (n > 1) duplicated.add(seq);
    }
    return new Outcome(LabKafka.isolationName(isolation), total, list, missing, duplicated);
  }

  private List<ClusterObserver.GroupPartition> groupOffsets(String run) {
    var committed =
        kafka.await(
            kafka
                .admin()
                .listConsumerGroupOffsets(DeliveryWorker.group(run))
                .partitionsToOffsetAndMetadata());
    var list = new ArrayList<ClusterObserver.GroupPartition>();
    committed.forEach(
        (tp, o) ->
            list.add(
                new ClusterObserver.GroupPartition(
                    tp.topic(), tp.partition(), o == null ? null : o.offset(), null, null)));
    list.sort(java.util.Comparator.comparingInt(ClusterObserver.GroupPartition::partition));
    return list;
  }

  /** Each run has its own group; empty ones from earlier runs would only clutter the view. */
  private void forgetOldGroups() {
    try {
      var old =
          kafka.await(kafka.admin().listGroups().all()).stream()
              .map(org.apache.kafka.clients.admin.GroupListing::groupId)
              .filter(g -> g.startsWith("lab.delivery."))
              .toList();
      if (!old.isEmpty()) kafka.admin().deleteConsumerGroups(old).all().get(5, TimeUnit.SECONDS);
    } catch (Exception stillActive) {
      // a group with members cannot be deleted; it goes next time
    }
  }

  private static NewTopic topic(String name) {
    return LabTopics.replicated(name, 3, 2);
  }

  /** Starts a worker JVM and waits for it to finish or crash. */
  private Worker launch(DeliveryWorker.Mode mode, String run, int crashAt, int number) {
    var command = new ArrayList<String>();
    command.add(ProcessHandle.current().info().command().orElse("java"));
    command.addAll(
        List.of("-Xmx128m", "-XX:TieredStopAtLevel=1", "-XX:+UseSerialGC", "-cp", classpath()));
    if (isBootJar())
      command.addAll(
          List.of(
              "-Dloader.main=" + DeliveryWorker.class.getName(),
              "org.springframework.boot.loader.launch.PropertiesLauncher"));
    else command.add(DeliveryWorker.class.getName());
    command.addAll(
        List.of(
            kafka.bootstrap(), mode.name(), run, String.valueOf(crashAt), String.valueOf(number)));
    var builder = new ProcessBuilder(command).redirectErrorStream(true);
    builder.environment().remove("JAVA_TOOL_OPTIONS"); // no tracing agent in the worker
    long started = System.nanoTime();
    var events = new ArrayList<String>();
    try {
      var process = builder.start();
      var reader =
          Thread.ofVirtual()
              .start(
                  () -> {
                    try (var in =
                        new BufferedReader(
                            new InputStreamReader(
                                process.getInputStream(), StandardCharsets.UTF_8))) {
                      String line;
                      while ((line = in.readLine()) != null)
                        if (line.startsWith("EVENT "))
                          synchronized (events) {
                            events.add(line.substring(6));
                          }
                        else if (line.contains("Exception"))
                          synchronized (events) {
                            events.add("log " + line);
                          }
                    } catch (java.io.IOException ignored) {
                      // the process is gone
                    }
                  });
      if (!process.waitFor(90, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        throw new ApiException(
            HttpStatus.GATEWAY_TIMEOUT,
            "WORKER_TIMED_OUT",
            "Worker " + number + " did not finish in 90 s");
      }
      reader.join(Duration.ofSeconds(2));
      int exit = process.exitValue();
      synchronized (events) {
        return new Worker(
            number,
            exit,
            exit == DeliveryWorker.CRASH_EXIT,
            (System.nanoTime() - started) / 1_000_000,
            List.copyOf(events));
      }
    } catch (java.io.IOException e) {
      throw new KafkaLabErrors.ScenarioFailed(
          "Could not start the worker JVM: " + e.getMessage(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static String classpath() {
    return System.getProperty("java.class.path");
  }

  /** Packaged: one Spring Boot jar, whose classes and libraries only its launcher can load. */
  private static boolean isBootJar() {
    var cp = classpath();
    if (cp.contains(File.pathSeparator) || !cp.endsWith(".jar")) return false;
    try (var jar = new JarFile(cp)) {
      return jar.getEntry("BOOT-INF/classes/") != null;
    } catch (java.io.IOException e) {
      return false;
    }
  }
}
