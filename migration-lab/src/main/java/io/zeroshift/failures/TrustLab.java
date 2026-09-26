package io.zeroshift.failures;

import static io.zeroshift.failures.Checks.check;
import static io.zeroshift.failures.Checks.compare;

import io.micrometer.core.instrument.MeterRegistry;
import io.zeroshift.eventlab.EventLabSettings;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * A fulfilment service ships an order when a PaymentCaptured event arrives. On the event lab's
 * broker (PLAINTEXT, no authorizer) anyone who can reach it can publish that event. On kafka-secure
 * every client authenticates and ACLs let only the payments principal write payment events.
 */
@Component
public class TrustLab implements FailureLab {
  static final String TOPIC = "lab.trust.payments";
  static final String ORDERS_TOPIC = "lab.trust.orders";
  static final String GROUP = "lab-trust-fulfilment";
  static final String DATABASE = "fl_trust";
  static final String OPEN = "open (kafka:9092, PLAINTEXT, no authorizer)";
  static final String SECURE = "secure (kafka-secure:9092, SASL/PLAIN + ACLs)";

  private final FailureDb db;
  private final EventLabSettings open;
  private final FailureLabSettings settings;
  private final MeterRegistry meters;
  private final JsonMapper json = JsonMapper.builder().build();

  public TrustLab(
      FailureDb db, EventLabSettings open, FailureLabSettings settings, MeterRegistry meters) {
    this.db = db;
    this.open = open;
    this.settings = settings;
    this.meters = meters;
  }

  @Override
  public String id() {
    return "trust";
  }

  @Override
  public String title() {
    return "Trust boundaries";
  }

  @Override
  public String summary() {
    return "Fulfilment ships whatever a PaymentCaptured event says was paid. A forged event on the open"
        + " broker ships an unpaid order; on the protected broker the same attempt never reaches the topic.";
  }

  @Override
  public String naive() {
    return "Trust the network: a PLAINTEXT broker with no authorizer. Every client is anonymous and may"
        + " write any topic, and a consumer cannot tell who produced a record.";
  }

  @Override
  public String correct() {
    return "Authenticate every client (SASL) and deny by default (StandardAuthorizer): one principal per"
        + " service, and ACLs that let each one write or read only its own topics and group.";
  }

  @Override
  public List<String> plan() {
    return List.of(
        "Recreate fl_trust and the open broker's "
            + TOPIC
            + ". Read the open broker's security settings and ACLs from its AdminClient. The payments service records a payment and publishes PaymentCaptured; fulfilment ships it.",
        "An attacker process with no credentials publishes a forged PaymentCaptured for an order nobody paid. The broker acknowledges it with an offset.",
        "Fulfilment consumes it and ships the order. Reconciliation against the payments table finds a shipment with no payment. The forged and the genuine record are compared byte for byte: nothing on a record says who produced it.",
        "On kafka-secure: create the topics and least-privilege ACLs as admin, then try every client: no credentials, a wrong password, checkout (writes orders only), fulfilment (reads only) and payments. Fulfilment on the secure broker ships only the genuine payment.",
        "Recover: reconcile shipments against payments, hold the forged shipment, record the incident; replay the attacker against the secure broker once more. Every shipment still active has a payment on both brokers.");
  }

  @Override
  public List<String> requires() {
    return List.of(
        "failure-postgres",
        "kafka-secure (SASL + ACLs)",
        "the event lab's Kafka (topic " + TOPIC + ")");
  }

  @Override
  public ObjectNode run(int stage, ObjectNode memo, Trace trace) throws Exception {
    db.requireConfigured();
    var result = json.createObjectNode();
    switch (stage) {
      case 0 -> {
        result.set("reset", reset());
        result.set("openBroker", json.valueToTree(openBrokerSecurity()));
        check(
            result,
            "open broker has an authorizer",
            "none",
            ((Map<?, ?>) openBrokerSecurity()).get("authorizer"),
            "none".equals(((Map<?, ?>) openBrokerSecurity()).get("authorizer")));
        var order = "ord-" + shortId();
        var paid =
            payAndPublish(
                () -> openProducer("payments-service"),
                OPEN,
                "anonymous (payments-service)",
                order,
                new BigDecimal("59.00"));
        result.set("genuinePayment", json.valueToTree(paid));
        var shipped = fulfil(openConsumer(), "open");
        result.set("fulfilment", json.valueToTree(shipped));
        memo.put("genuineOrder", order);
        var reconciliation = reconcile("open");
        result.set("reconciliation", json.valueToTree(reconciliation));
        check(result, "genuine order shipped", 1, shipped.size());
        check(result, "shipments without a payment", 0L, reconciliation.get("unpaid"));
      }
      case 1 -> {
        var forged = "ord-" + shortId();
        memo.put("forgedOrder", forged);
        var decision =
            publish(
                () -> openProducer("attacker"),
                OPEN,
                "anonymous (attacker)",
                TOPIC,
                forged,
                event(forged, new BigDecimal("1999.00")));
        record(decision);
        result.set("attackerDecision", json.valueToTree(decision));
        trace.event("forged event accepted", Map.of("offset", String.valueOf(decision.offset())));
        check(result, "forged event accepted by the open broker", "ALLOWED", decision.decision());
        check(result, "payment on record for the forged order", 0L, paymentCount(forged));
        compare(
            result,
            "Who may publish payment events",
            "anyone who can reach the broker: no authentication, no authorizer",
            null);
        compare(result, "Forged PaymentCaptured", "accepted at offset " + decision.offset(), null);
      }
      case 2 -> {
        var shipped = fulfil(openConsumer(), "open");
        result.set("fulfilment", json.valueToTree(shipped));
        var reconciliation = reconcile("open");
        result.set("reconciliation", json.valueToTree(reconciliation));
        result.set(
            "recordsSideBySide",
            json.valueToTree(
                records(
                    memo.path("genuineOrder").asString(), memo.path("forgedOrder").asString())));
        check(
            result,
            "forged order shipped",
            true,
            shipped.stream()
                .anyMatch(s -> s.get("order_id").equals(memo.path("forgedOrder").asString())));
        check(result, "shipments without a payment", 1L, reconciliation.get("unpaid"));
        compare(
            result,
            "Consequence",
            "order "
                + memo.path("forgedOrder").asString()
                + " shipped, worth "
                + reconciliation.get("unpaidAmount")
                + ", never paid",
            null);
        compare(
            result,
            "Can the consumer tell?",
            "no: the forged record has the same shape, key and headers as a genuine one",
            null);
      }
      case 3 -> {
        requireSecure();
        result.set("setup", json.valueToTree(secureSetup()));
        result.set("acls", json.valueToTree(acls()));
        var attempts = new ArrayList<SecurityDecision>();
        var forged = memo.path("forgedOrder").asString("ord-forged");
        attempts.add(
            publish(
                () -> secureProducer("attacker", null),
                SECURE,
                "no credentials",
                TOPIC,
                forged,
                event(forged, new BigDecimal("1999.00"))));
        attempts.add(
            publish(
                () -> secureProducer("payments", "guessed-password"),
                SECURE,
                "payments with a wrong password",
                TOPIC,
                forged,
                event(forged, new BigDecimal("1999.00"))));
        attempts.add(
            publish(
                () -> secureProducer("checkout", settings.password("checkout")),
                SECURE,
                "User:checkout",
                TOPIC,
                forged,
                event(forged, new BigDecimal("1999.00"))));
        attempts.add(
            publish(
                () -> secureProducer("checkout", settings.password("checkout")),
                SECURE,
                "User:checkout",
                ORDERS_TOPIC,
                forged,
                "{\"type\":\"OrderPlaced\",\"orderId\":\"" + forged + "\"}"));
        attempts.add(
            publish(
                () -> secureProducer("fulfilment", settings.password("fulfilment")),
                SECURE,
                "User:fulfilment",
                TOPIC,
                forged,
                event(forged, new BigDecimal("1999.00"))));
        attempts.add(snoop());
        var order = "ord-" + shortId();
        memo.put("secureOrder", order);
        var genuine =
            payAndPublish(
                () -> secureProducer("payments", settings.password("payments")),
                SECURE,
                "User:payments",
                order,
                new BigDecimal("59.00"));
        attempts.add((SecurityDecision) genuine.get("decision"));
        for (var a : attempts) if (a != genuine.get("decision")) record(a);
        result.set("attempts", json.valueToTree(attempts));
        var shipped = fulfil(secureConsumer(), "secure");
        result.set("fulfilment", json.valueToTree(shipped));
        var reconciliation = reconcile("secure");
        result.set("reconciliation", json.valueToTree(reconciliation));
        check(
            result,
            "no credentials",
            "DENIED (connection)",
            attempts.get(0).decision() + " (" + attempts.get(0).stage() + ")",
            attempts.get(0).decision().equals("DENIED"));
        check(
            result,
            "wrong password",
            "DENIED (authentication)",
            attempts.get(1).decision() + " (" + attempts.get(1).stage() + ")");
        check(
            result,
            "checkout writing payment events",
            "DENIED (authorization)",
            attempts.get(2).decision() + " (" + attempts.get(2).stage() + ")");
        check(
            result, "checkout writing its own orders topic", "ALLOWED", attempts.get(3).decision());
        check(
            result,
            "fulfilment writing payment events",
            "DENIED (authorization)",
            attempts.get(4).decision() + " (" + attempts.get(4).stage() + ")");
        check(
            result,
            "checkout reading payment events",
            "DENIED (authorization)",
            attempts.get(5).decision() + " (" + attempts.get(5).stage() + ")");
        check(result, "payments writing payment events", "ALLOWED", attempts.get(6).decision());
        check(
            result,
            "secure fulfilment shipped only the genuine order",
            List.of(order),
            shipped.stream().map(s -> s.get("order_id")).toList());
        check(result, "secure shipments without a payment", 0L, reconciliation.get("unpaid"));
        trace.event(
            "secure broker decisions",
            Map.of(
                "denied",
                String.valueOf(
                    attempts.stream().filter(a -> a.decision().equals("DENIED")).count())));
        compare(
            result,
            "Who may publish payment events",
            null,
            "only User:payments (ACL: WRITE on " + TOPIC + ")");
        compare(
            result,
            "Forged PaymentCaptured",
            null,
            "rejected: "
                + attempts.get(0).error()
                + ", "
                + attempts.get(1).error()
                + ", "
                + attempts.get(2).error()
                + "; never on the topic");
        compare(result, "Consequence", null, "nothing shipped without a payment");
        compare(
            result,
            "Least privilege",
            "every client may do everything",
            "checkout writes orders only; fulfilment reads payments only; neither can write payments");
      }
      case 4 -> {
        var held = holdUnpaid("open");
        result.set("heldShipments", json.valueToTree(held));
        var again =
            publish(
                () -> secureProducer("checkout", settings.password("checkout")),
                SECURE,
                "User:checkout",
                TOPIC,
                "ord-" + shortId(),
                event("ord-replayed", new BigDecimal("1999.00")));
        record(again);
        result.set("attackReplayedOnSecure", json.valueToTree(again));
        var secureShipped = fulfil(secureConsumer(), "secure");
        result.set("secureFulfilmentAfterReplay", json.valueToTree(secureShipped));
        var open = reconcile("open");
        var secure = reconcile("secure");
        result.set("reconciliation", json.valueToTree(Map.of("open", open, "secure", secure)));
        result.set("decisions", json.valueToTree(decisions()));
        check(result, "forged shipments held", 1, held.size());
        check(result, "active shipments without a payment (open broker)", 0L, open.get("unpaid"));
        check(
            result, "active shipments without a payment (secure broker)", 0L, secure.get("unpaid"));
        check(result, "the attack replayed on the secure broker", "DENIED", again.decision());
        check(result, "new shipments from it", 0, secureShipped.size());
        compare(
            result,
            "Recovery",
            "after the fact: reconcile shipments against payments and hold what was forged ("
                + held.size()
                + " held)",
            "nothing to recover: the forged event never reached the topic");
      }
      default -> throw new IllegalArgumentException("No stage " + stage);
    }
    return result;
  }

  // ---- the services
  // -------------------------------------------------------------------------------

  /** The payments service: records the payment, then publishes the event. */
  private Map<String, Object> payAndPublish(
      Supplier<KafkaProducer<String, String>> producer,
      String broker,
      String principal,
      String order,
      BigDecimal amount)
      throws SQLException {
    try (var c = db.connect(DATABASE);
        var s = c.prepareStatement("INSERT INTO payment(order_id, amount) VALUES (?, ?)")) {
      s.setString(1, order);
      s.setBigDecimal(2, amount);
      s.executeUpdate();
    }
    var decision = publish(producer, broker, principal, TOPIC, order, event(order, amount));
    record(decision);
    var out = new LinkedHashMap<String, Object>();
    out.put("order", order);
    out.put("amount", amount);
    out.put("decision", decision);
    return out;
  }

  /**
   * One attempt to write, as the broker answered it. An outcome that is not a decision (an
   * authenticated client timing out: the broker was slow) is tried once more with a new client.
   */
  private SecurityDecision publish(
      Supplier<KafkaProducer<String, String>> client,
      String broker,
      String principal,
      String topic,
      String key,
      String value) {
    var decision = publishOnce(client.get(), broker, principal, topic, key, value);
    if (decision.decision().equals("FAILED"))
      decision = publishOnce(client.get(), broker, principal, topic, key, value);
    return decision;
  }

  private SecurityDecision publishOnce(
      KafkaProducer<String, String> producer,
      String broker,
      String principal,
      String topic,
      String key,
      String value) {
    boolean authenticated = !principal.equals("no credentials");
    try (producer) {
      var meta = producer.send(new ProducerRecord<>(topic, key, value)).get(30, TimeUnit.SECONDS);
      meters
          .counter(
              "zeroshift.failure_lab.security.decisions",
              "broker",
              broker.startsWith("open") ? "open" : "secure",
              "decision",
              "ALLOWED")
          .increment();
      return SecurityDecision.allowed(
          broker,
          principal,
          "WRITE",
          "topic " + topic,
          meta.offset(),
          "partition " + meta.partition() + ", offset " + meta.offset());
    } catch (Exception e) {
      var d = SecurityDecision.from(broker, principal, "WRITE", "topic " + topic, e, authenticated);
      meters
          .counter(
              "zeroshift.failure_lab.security.decisions",
              "broker",
              broker.startsWith("open") ? "open" : "secure",
              "decision",
              d.decision())
          .increment();
      return d;
    }
  }

  /** Checkout trying to read payment events on the secure broker with a group of its own. */
  private SecurityDecision snoop() {
    var p = secureClient("checkout", settings.password("checkout"));
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "lab-trust-snoop");
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "8000");
    try (var consumer =
        new KafkaConsumer<>(p, new StringDeserializer(), new StringDeserializer())) {
      consumer.subscribe(List.of(TOPIC));
      long deadline = System.currentTimeMillis() + 10_000;
      while (System.currentTimeMillis() < deadline) {
        var records = consumer.poll(Duration.ofMillis(500));
        if (!records.isEmpty())
          return SecurityDecision.allowed(
              SECURE,
              "User:checkout",
              "READ",
              "topic " + TOPIC,
              null,
              records.count() + " records read");
      }
      return SecurityDecision.allowed(
          SECURE,
          "User:checkout",
          "READ",
          "topic " + TOPIC,
          null,
          "no error and no records within 10 s");
    } catch (Exception e) {
      return SecurityDecision.from(
          SECURE, "User:checkout", "READ", "topic " + TOPIC + ", group lab-trust-snoop", e, true);
    }
  }

  /** Fulfilment: ships every PaymentCaptured it reads, from its group's committed offset on. */
  private List<Map<String, Object>> fulfil(KafkaConsumer<String, String> consumer, String broker)
      throws Exception {
    var shipped = new ArrayList<Map<String, Object>>();
    try (consumer;
        var c = db.connect(DATABASE)) {
      var tp = new TopicPartition(TOPIC, 0);
      consumer.assign(List.of(tp));
      var committed = consumer.committed(java.util.Set.of(tp)).get(tp);
      consumer.seek(tp, committed == null ? 0 : committed.offset());
      long end = consumer.endOffsets(List.of(tp)).get(tp);
      long deadline = System.currentTimeMillis() + 20_000;
      while (consumer.position(tp) < end) {
        if (System.currentTimeMillis() > deadline)
          throw new IllegalStateException("fulfilment did not reach offset " + end);
        for (var r : consumer.poll(Duration.ofMillis(300))) {
          var e = json.readTree(r.value());
          try (var s =
              c.prepareStatement(
                  "INSERT INTO shipment(order_id, broker, amount, source_offset) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING")) {
            s.setString(1, e.path("orderId").asString());
            s.setString(2, broker);
            s.setBigDecimal(3, new BigDecimal(e.path("amount").asString()));
            s.setLong(4, r.offset());
            if (s.executeUpdate() == 1) {
              var row = new LinkedHashMap<String, Object>();
              row.put("order_id", e.path("orderId").asString());
              row.put("amount", e.path("amount").asString());
              row.put("offset", r.offset());
              shipped.add(row);
            }
          }
        }
        consumer.commitSync(Map.of(tp, new OffsetAndMetadata(consumer.position(tp))));
      }
    }
    return shipped;
  }

  /** Shipments of one broker's fulfilment against the payments table. */
  private Map<String, Object> reconcile(String broker) throws SQLException {
    try (var c = db.connect(DATABASE)) {
      var unpaid =
          FailureDb.rows(
              c,
              "SELECT s.order_id, s.amount, s.source_offset, s.status FROM shipment s"
                  + " WHERE s.broker = ? AND s.status = 'SHIPPED' AND NOT EXISTS (SELECT 1 FROM payment p WHERE p.order_id = s.order_id)",
              broker);
      var totals =
          FailureDb.one(
              c,
              "SELECT count(*) AS shipments, count(*) FILTER (WHERE status = 'SHIPPED') AS active FROM shipment WHERE broker = ?",
              broker);
      var out = new LinkedHashMap<String, Object>();
      out.put("broker", broker);
      out.put("shipments", totals.get("shipments"));
      out.put("active", totals.get("active"));
      out.put("unpaid", (long) unpaid.size());
      out.put(
          "unpaidAmount",
          unpaid.stream()
              .map(r -> (BigDecimal) r.get("amount"))
              .reduce(BigDecimal.ZERO, BigDecimal::add));
      out.put("unpaidShipments", unpaid);
      return out;
    }
  }

  private List<Map<String, Object>> holdUnpaid(String broker) throws SQLException {
    try (var c = db.connect(DATABASE)) {
      return FailureDb.rows(
          c,
          "UPDATE shipment s SET status = 'HELD', note = 'no payment on record: forged PaymentCaptured at offset ' || s.source_offset"
              + " WHERE s.broker = ? AND s.status = 'SHIPPED' AND NOT EXISTS (SELECT 1 FROM payment p WHERE p.order_id = s.order_id)"
              + " RETURNING order_id, amount, status, note",
          broker);
    }
  }

  private long paymentCount(String order) throws SQLException {
    try (var c = db.connect(DATABASE)) {
      return ((Number)
              FailureDb.one(c, "SELECT count(*) AS n FROM payment WHERE order_id = ?", order)
                  .get("n"))
          .longValue();
    }
  }

  /** Both records as fulfilment received them: key, headers and value. */
  private List<Map<String, Object>> records(String genuine, String forged) {
    var p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, open.kafkaBootstrap());
    var out = new ArrayList<Map<String, Object>>();
    try (var consumer =
        new KafkaConsumer<>(p, new StringDeserializer(), new StringDeserializer())) {
      var tp = new TopicPartition(TOPIC, 0);
      consumer.assign(List.of(tp));
      consumer.seekToBeginning(List.of(tp));
      long end = consumer.endOffsets(List.of(tp)).get(tp);
      long deadline = System.currentTimeMillis() + 10_000;
      while (consumer.position(tp) < end && System.currentTimeMillis() < deadline)
        for (var r : consumer.poll(Duration.ofMillis(300)))
          if (r.key().equals(genuine) || r.key().equals(forged)) {
            var row = new LinkedHashMap<String, Object>();
            row.put("which", r.key().equals(forged) ? "forged (attacker)" : "genuine (payments)");
            row.put("offset", r.offset());
            row.put("key", r.key());
            row.put("headers", r.headers().toArray().length);
            row.put("value", r.value());
            row.put("timestampType", r.timestampType().name);
            row.put("producerIdentity", "not on the record");
            out.add(row);
          }
    }
    return out;
  }

  private String event(String order, BigDecimal amount) {
    return json.createObjectNode()
        .put("eventId", UUID.randomUUID().toString())
        .put("type", "PaymentCaptured")
        .put("orderId", order)
        .put("amount", amount.toPlainString())
        .put("occurredAt", Instant.now().toString())
        .toString();
  }

  // ---- brokers
  // --------------------------------------------------------------------------------------

  private KafkaProducer<String, String> openProducer(String clientId) {
    var p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, open.kafkaBootstrap());
    return producer(p, clientId, true);
  }

  private KafkaProducer<String, String> secureProducer(String principal, String password) {
    return producer(secureClient(principal, password), principal, password != null);
  }

  /**
   * @param answered whether the broker will ever answer this client. One without SASL credentials
   *     is never served by kafka-secure, so it gives up after 5 s; every other client waits long
   *     enough for a busy broker's real answer.
   */
  private static KafkaProducer<String, String> producer(Properties p, String clientId, boolean answered) {
    p.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
    p.put(ProducerConfig.ACKS_CONFIG, "all");
    p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, answered ? "15000" : "5000");
    p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
    p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "20000");
    p.put(ProducerConfig.LINGER_MS_CONFIG, "0");
    return new KafkaProducer<>(p, new StringSerializer(), new StringSerializer());
  }

  private KafkaConsumer<String, String> openConsumer() {
    var p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, open.kafkaBootstrap());
    p.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    return new KafkaConsumer<>(p, new StringDeserializer(), new StringDeserializer());
  }

  private KafkaConsumer<String, String> secureConsumer() {
    var p = secureClient("fulfilment", settings.password("fulfilment"));
    p.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    return new KafkaConsumer<>(p, new StringDeserializer(), new StringDeserializer());
  }

  /**
   * A client of kafka-secure: SASL/PLAIN as {@code principal}, or no SASL at all when password is
   * null.
   */
  private Properties secureClient(String principal, String password) {
    var p = new Properties();
    p.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, settings.secureKafka());
    p.put(CommonClientConfigs.CLIENT_ID_CONFIG, principal);
    if (password != null) {
      p.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
      p.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
      p.put(
          SaslConfigs.SASL_JAAS_CONFIG,
          "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
              + principal
              + "\" password=\""
              + password
              + "\";");
    }
    return p;
  }

  private Admin openAdmin() {
    var p = new Properties();
    p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, open.kafkaBootstrap());
    p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
    p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
    return Admin.create(p);
  }

  private Admin secureAdmin() {
    var p = secureClient("admin", settings.password("admin"));
    p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
    p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
    return Admin.create(p);
  }

  private void requireSecure() {
    if (!settings.secureKafkaConfigured())
      throw new IllegalStateException(
          "kafka-secure is not configured. Start it with: docker compose --profile failure-lab up -d");
  }

  /** The open broker's own answer about its security: listener protocols, authorizer, ACLs. */
  private Object openBrokerSecurity() throws Exception {
    try (var admin = openAdmin()) {
      var node = admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).iterator().next();
      var resource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(node.id()));
      var config =
          admin.describeConfigs(List.of(resource)).all().get(10, TimeUnit.SECONDS).get(resource);
      var out = new LinkedHashMap<String, Object>();
      out.put("broker", node.host() + ":" + node.port());
      out.put(
          "listener.security.protocol.map", config.get("listener.security.protocol.map").value());
      var authorizer = config.get("authorizer.class.name");
      out.put(
          "authorizer",
          authorizer == null || authorizer.value() == null || authorizer.value().isBlank()
              ? "none"
              : authorizer.value());
      try {
        admin.describeAcls(AclBindingFilter.ANY).values().get(10, TimeUnit.SECONDS);
        out.put("describeAcls", "answered");
      } catch (ExecutionException e) {
        out.put(
            "describeAcls",
            e.getCause().getClass().getSimpleName() + ": " + e.getCause().getMessage());
      }
      return out;
    }
  }

  private Map<String, Object> secureSetup() throws Exception {
    try (var admin = secureAdmin()) {
      var existing = admin.listTopics().names().get(10, TimeUnit.SECONDS);
      var create = new ArrayList<NewTopic>();
      for (var t : List.of(TOPIC, ORDERS_TOPIC))
        if (!existing.contains(t)) create.add(new NewTopic(t, 1, (short) 1));
      if (!create.isEmpty()) admin.createTopics(create).all().get(15, TimeUnit.SECONDS);
      var bindings =
          List.of(
              acl(ResourceType.TOPIC, TOPIC, "payments", AclOperation.WRITE),
              acl(ResourceType.TOPIC, TOPIC, "payments", AclOperation.DESCRIBE),
              acl(ResourceType.TOPIC, TOPIC, "fulfilment", AclOperation.READ),
              acl(ResourceType.TOPIC, TOPIC, "fulfilment", AclOperation.DESCRIBE),
              acl(ResourceType.GROUP, GROUP, "fulfilment", AclOperation.READ),
              acl(ResourceType.TOPIC, ORDERS_TOPIC, "checkout", AclOperation.WRITE),
              acl(ResourceType.TOPIC, ORDERS_TOPIC, "checkout", AclOperation.DESCRIBE));
      admin.createAcls(bindings).all().get(15, TimeUnit.SECONDS);
      // ACLs reach the authorizer through the metadata log; give it a moment.
      Thread.sleep(1000);
      return Map.of(
          "topics",
          List.of(TOPIC, ORDERS_TOPIC),
          "aclsCreated",
          bindings.size(),
          "superUser",
          "User:admin (the operator and the broker itself)");
    }
  }

  private static AclBinding acl(ResourceType type, String name, String principal, AclOperation op) {
    return new AclBinding(
        new ResourcePattern(type, name, PatternType.LITERAL),
        new AccessControlEntry("User:" + principal, "*", op, AclPermissionType.ALLOW));
  }

  private List<Map<String, Object>> acls() throws Exception {
    try (var admin = secureAdmin()) {
      return admin.describeAcls(AclBindingFilter.ANY).values().get(10, TimeUnit.SECONDS).stream()
          .map(
              b -> {
                var row = new LinkedHashMap<String, Object>();
                row.put("principal", b.entry().principal());
                row.put("permission", b.entry().permissionType().name());
                row.put("operation", b.entry().operation().name());
                row.put("resource", b.pattern().resourceType().name() + " " + b.pattern().name());
                row.put("pattern", b.pattern().patternType().name());
                return (Map<String, Object>) row;
              })
          .sorted(
              (a, b) ->
                  (a.get("principal") + "" + a.get("resource"))
                      .compareTo(b.get("principal") + "" + b.get("resource")))
          .toList();
    }
  }

  private void record(SecurityDecision d) throws SQLException {
    try (var c = db.connect(DATABASE);
        var s =
            c.prepareStatement(
                "INSERT INTO security_decision(broker, principal, operation, resource, decision, stage, error, detail, kafka_offset)"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
      s.setString(1, d.broker());
      s.setString(2, d.principal());
      s.setString(3, d.operation());
      s.setString(4, d.resource());
      s.setString(5, d.decision());
      s.setString(6, d.stage());
      s.setString(7, d.error());
      s.setString(8, d.detail());
      s.setObject(9, d.offset());
      s.executeUpdate();
    }
  }

  private List<Map<String, Object>> decisions() throws SQLException {
    try (var c = db.connect(DATABASE)) {
      return FailureDb.rows(
          c,
          "SELECT at, broker, principal, operation, resource, decision, stage, error, kafka_offset FROM security_decision ORDER BY id DESC LIMIT 30");
    }
  }

  // ---- inspector and reset ----------------------------------------------------------------------

  @Override
  public ObjectNode state() throws Exception {
    var state = json.createObjectNode();
    state.put("secureBrokerConfigured", settings.secureKafkaConfigured());
    if (!db.exists(DATABASE)) {
      state.put("ready", false);
      state.put("note", "Not set up yet: run the first stage.");
      return state;
    }
    state.put("ready", true);
    state.set("decisions", json.valueToTree(decisions()));
    state.set("reconciliation", json.valueToTree(List.of(reconcile("open"), reconcile("secure"))));
    try (var c = db.connect(DATABASE)) {
      state.set(
          "shipments",
          json.valueToTree(
              FailureDb.rows(
                  c,
                  "SELECT order_id, broker, amount, source_offset, status, note, at FROM shipment ORDER BY at DESC LIMIT 20")));
      state.set(
          "payments",
          json.valueToTree(
              FailureDb.rows(
                  c, "SELECT order_id, amount, at FROM payment ORDER BY at DESC LIMIT 20")));
    }
    if (settings.secureKafkaConfigured())
      try {
        state.set("secureAcls", json.valueToTree(acls()));
      } catch (Exception e) {
        state.put("secureAcls", "unavailable: " + SecurityDecision.root(e).getMessage());
      }
    return state;
  }

  @Override
  public ObjectNode reset() throws Exception {
    db.requireConfigured();
    db.drop(DATABASE);
    db.create(DATABASE);
    db.execute(
        DATABASE,
        "CREATE TABLE payment (order_id text PRIMARY KEY, amount numeric(12,2) NOT NULL, at timestamptz NOT NULL DEFAULT clock_timestamp())",
        "CREATE TABLE shipment (order_id text NOT NULL, broker text NOT NULL, amount numeric(12,2) NOT NULL, source_offset bigint NOT NULL,"
            + " status text NOT NULL DEFAULT 'SHIPPED', note text, at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY (order_id, broker))",
        "CREATE TABLE security_decision (id bigserial PRIMARY KEY, at timestamptz NOT NULL DEFAULT clock_timestamp(), broker text NOT NULL,"
            + " principal text NOT NULL, operation text NOT NULL, resource text NOT NULL, decision text NOT NULL, stage text, error text, detail text, kafka_offset bigint)");
    var out = json.createObjectNode();
    try (var admin = openAdmin()) {
      recreate(admin, TOPIC);
      deleteGroup(admin, GROUP);
    }
    out.put("open", TOPIC + " recreated, group " + GROUP + " deleted");
    if (settings.secureKafkaConfigured()) {
      try (var admin = secureAdmin()) {
        for (var t : List.of(TOPIC, ORDERS_TOPIC))
          try {
            admin.deleteTopics(List.of(t)).all().get(15, TimeUnit.SECONDS);
          } catch (ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) throw e;
          }
        deleteGroup(admin, GROUP);
        deleteGroup(admin, "lab-trust-snoop");
        var removed =
            admin.deleteAcls(List.of(AclBindingFilter.ANY)).all().get(15, TimeUnit.SECONDS);
        out.put("secure", "topics deleted, " + removed.size() + " ACLs removed");
      }
    }
    out.put("database", DATABASE + " recreated");
    return out;
  }

  private static void recreate(Admin admin, String topic) throws Exception {
    try {
      admin.deleteTopics(List.of(topic)).all().get(15, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) throw e;
    }
    long deadline = System.currentTimeMillis() + 30_000;
    while (true) {
      try {
        admin
            .createTopics(
                List.of(
                    new NewTopic(topic, 1, (short) 1).configs(Map.of("retention.ms", "86400000"))))
            .all()
            .get(15, TimeUnit.SECONDS);
        return;
      } catch (ExecutionException e) {
        if (System.currentTimeMillis() > deadline) throw e;
        Thread.sleep(500);
      }
    }
  }

  private static void deleteGroup(Admin admin, String group) throws Exception {
    try {
      admin.deleteConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      if (!(e.getCause() instanceof GroupIdNotFoundException)) throw e;
    }
  }

  private static String shortId() {
    return UUID.randomUUID().toString().substring(0, 8);
  }
}
