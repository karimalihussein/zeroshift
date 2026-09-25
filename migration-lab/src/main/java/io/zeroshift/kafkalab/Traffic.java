package io.zeroshift.kafkalab;

import io.zeroshift.platform.web.ApiException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Keeps producing and consuming on lab.replicated while brokers fail, and counts what happened to
 * every record: acknowledged, failed, consumed, consumed twice, or acknowledged and never consumed
 * (lost). The producer is the safe one: acks=all and idempotent.
 */
@Component
public class Traffic {
  public static final String GROUP = "lab.replicated.reader";
  static final int RATE_PER_SECOND = 20;

  public record Second(long at, int acked, int failed, int consumed, long maxLatencyMs) {}

  /**
   * @param lostAcknowledged acknowledged to the producer but never consumed; only known once
   *     stopped and drained
   */
  public record View(
      boolean running,
      String run,
      Instant startedAt,
      Instant stoppedAt,
      int ratePerSecond,
      long sent,
      long acked,
      long failed,
      long consumed,
      long duplicates,
      long retries,
      long inFlight,
      long maxLatencyMs,
      Map<String, Long> errors,
      List<Second> timeline,
      Long lostAcknowledged) {}

  private final LabKafka kafka;
  private final LabSetup setup;
  private final KafkaLabRuns runs;

  private volatile Session session;

  public Traffic(LabKafka kafka, LabSetup setup, KafkaLabRuns runs) {
    this.kafka = kafka;
    this.setup = setup;
    this.runs = runs;
  }

  public synchronized View start() {
    if (session != null && session.running) throw busy();
    setup.ensureStanding();
    try {
      kafka.await(kafka.admin().deleteConsumerGroups(List.of(GROUP)).all());
    } catch (RuntimeException absentOrEmpty) {
      // first run, or no group yet
    }
    session = new Session();
    session.begin();
    return view();
  }

  public synchronized View stop() {
    var s = session;
    if (s == null || !s.running)
      throw new ApiException(HttpStatus.CONFLICT, "TRAFFIC_NOT_RUNNING", "Traffic is not running");
    s.end();
    var view = view();
    runs.record(
        KafkaLabRuns.FAILOVER,
        "traffic",
        view.acked()
            + " acknowledged, "
            + view.failed()
            + " failed, "
            + view.lostAcknowledged()
            + " acknowledged but lost, "
            + view.duplicates()
            + " consumed twice",
        view);
    return view;
  }

  public View view() {
    var s = session;
    return s == null
        ? new View(
            false,
            null,
            null,
            null,
            RATE_PER_SECOND,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            Map.of(),
            List.of(),
            null)
        : s.view();
  }

  static ApiException busy() {
    return new ApiException(HttpStatus.CONFLICT, "TRAFFIC_RUNNING", "Traffic is already running");
  }

  private final class Session {
    final String run = UUID.randomUUID().toString().substring(0, 8);
    final Instant startedAt = Instant.now();
    volatile Instant stoppedAt;
    volatile boolean running;
    volatile boolean consuming;
    final AtomicLong sent = new AtomicLong();
    final AtomicLong acked = new AtomicLong();
    final AtomicLong failed = new AtomicLong();
    final AtomicLong consumed = new AtomicLong();
    final AtomicLong duplicates = new AtomicLong();
    final AtomicLong maxLatency = new AtomicLong();
    volatile long retries;
    final BitSet ackedSeqs = new BitSet();
    final BitSet consumedSeqs = new BitSet();
    final Map<String, Long> errors = new ConcurrentHashMap<>();
    final TreeMap<Long, long[]> seconds = new TreeMap<>(); // acked, failed, consumed, max latency
    volatile Long lost;
    KafkaProducer<String, String> producer;
    Thread sender;
    Thread reader;

    void begin() {
      producer =
          kafka.producer(
              Map.of(
                  ProducerConfig.ACKS_CONFIG, "all",
                  ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                  ProducerConfig.LINGER_MS_CONFIG, 5,
                  ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 4000,
                  ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000,
                  ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100,
                  ProducerConfig.CLIENT_ID_CONFIG, "lab-traffic-" + run));
      running = true;
      consuming = true;
      sender = Thread.ofVirtual().name("lab-traffic-producer").start(this::produce);
      reader = Thread.ofVirtual().name("lab-traffic-consumer").start(this::consume);
    }

    void produce() {
      long seq = 0;
      long interval = 1000 / RATE_PER_SECOND;
      while (running) {
        long n = ++seq;
        long sentAt = System.nanoTime();
        try {
          producer.send(
              new ProducerRecord<>(LabTopics.REPLICATED, "k" + (n % 12), run + ":" + n),
              (metadata, e) -> {
                long ms = (System.nanoTime() - sentAt) / 1_000_000;
                if (e == null) {
                  acked.incrementAndGet();
                  synchronized (ackedSeqs) {
                    ackedSeqs.set((int) n);
                  }
                  maxLatency.accumulateAndGet(ms, Math::max);
                  bucket(0, 1, ms);
                } else {
                  failed.incrementAndGet();
                  errors.merge(e.getClass().getSimpleName(), 1L, Long::sum);
                  bucket(1, 1, ms);
                }
              });
          sent.incrementAndGet();
        } catch (RuntimeException e) {
          // Could not even hand the record to the producer (metadata unavailable for max.block.ms).
          sent.incrementAndGet();
          failed.incrementAndGet();
          errors.merge(e.getClass().getSimpleName(), 1L, Long::sum);
          bucket(1, 1, 0);
        }
        retries = retryTotal();
        LabKafka.sleep(interval);
      }
    }

    void consume() {
      try (var consumer =
          kafka.consumer(
              Map.of(
                  ConsumerConfig.GROUP_ID_CONFIG,
                  GROUP,
                  ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                  "earliest",
                  ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                  true,
                  ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                  1000,
                  ConsumerConfig.CLIENT_ID_CONFIG,
                  "lab-traffic-reader-" + run))) {
        consumer.subscribe(List.of(LabTopics.REPLICATED));
        while (consuming) {
          for (var r : consumer.poll(Duration.ofMillis(200))) {
            var parts = r.value().split(":");
            if (parts.length != 2 || !parts[0].equals(run)) continue; // an earlier run's record
            int n = Integer.parseInt(parts[1]);
            consumed.incrementAndGet();
            synchronized (consumedSeqs) {
              if (consumedSeqs.get(n)) duplicates.incrementAndGet();
              consumedSeqs.set(n);
            }
            bucket(2, 1, 0);
          }
        }
      } catch (RuntimeException e) {
        errors.merge("consumer " + e.getClass().getSimpleName(), 1L, Long::sum);
      }
    }

    void end() {
      running = false;
      join(sender, Duration.ofSeconds(3));
      // Every record still in flight gets its answer (or its delivery timeout) before counting.
      producer.close(Duration.ofSeconds(35));
      retries = retryTotal();
      long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
      while (System.nanoTime() < deadline && missing() > 0) LabKafka.sleep(200);
      consuming = false;
      join(reader, Duration.ofSeconds(5));
      lost = missing();
      stoppedAt = Instant.now();
    }

    long missing() {
      BitSet gone;
      synchronized (ackedSeqs) {
        gone = (BitSet) ackedSeqs.clone();
      }
      synchronized (consumedSeqs) {
        gone.andNot(consumedSeqs);
      }
      return gone.cardinality();
    }

    long retryTotal() {
      try {
        return producer.metrics().entrySet().stream()
            .filter(
                e ->
                    e.getKey().name().equals("record-retry-total")
                        && e.getKey().group().equals("producer-metrics"))
            .mapToLong(e -> ((Number) e.getValue().metricValue()).longValue())
            .sum();
      } catch (RuntimeException closed) {
        return retries;
      }
    }

    void bucket(int index, int count, long latency) {
      long second = Instant.now().getEpochSecond();
      synchronized (seconds) {
        var b = seconds.computeIfAbsent(second, s -> new long[4]);
        b[index] += count;
        b[3] = Math.max(b[3], latency);
        while (seconds.size() > 120) seconds.pollFirstEntry();
      }
    }

    View view() {
      var timeline = new ArrayList<Second>();
      synchronized (seconds) {
        seconds.forEach(
            (at, b) -> timeline.add(new Second(at, (int) b[0], (int) b[1], (int) b[2], b[3])));
      }
      return new View(
          running,
          run,
          startedAt,
          stoppedAt,
          RATE_PER_SECOND,
          sent.get(),
          acked.get(),
          failed.get(),
          consumed.get(),
          duplicates.get(),
          retries,
          Math.max(0, sent.get() - acked.get() - failed.get()),
          maxLatency.get(),
          Map.copyOf(errors),
          timeline.size() > 90 ? timeline.subList(timeline.size() - 90, timeline.size()) : timeline,
          lost);
    }

    private static void join(Thread t, Duration timeout) {
      try {
        t.join(timeout);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
