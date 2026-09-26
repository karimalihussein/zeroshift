package io.zeroshift.payment;

import static io.zeroshift.platform.testing.ServiceTestSupport.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.contracts.PaymentCommand.*;
import io.zeroshift.contracts.PaymentEvent.*;
import io.zeroshift.contracts.Topics;
import io.zeroshift.payment.infrastructure.GatewayControl;
import io.zeroshift.platform.testing.CommerceStack;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;
import tools.jackson.databind.JsonNode;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PaymentServiceIT {
  static final GenericContainer<?> GATEWAY =
      new GenericContainer<>("wiremock/wiremock:3.13.1")
          .withExposedPorts(8080)
          .withCopyFileToContainer(
              MountableFile.forHostPath("../infra/payment-gateway/mappings"),
              "/home/wiremock/mappings")
          .waitingFor(Wait.forHttp("/__admin/health").forPort(8080));

  @DynamicPropertySource
  static void stack(DynamicPropertyRegistry registry) {
    GATEWAY.start();
    configure(registry, "payments");
    registry.add("payment.gateway-url", PaymentServiceIT::gatewayUrl);
  }

  @Autowired KafkaTemplate<String, String> kafka;
  @Autowired JdbcTemplate jdbc;
  @Autowired GatewayControl gateway;
  @LocalServerPort int port;

  @BeforeEach
  void connector() { // after the service's migrations created the publication and slot
    CommerceStack.registerOutboxConnector("payments");
  }

  @Test
  void generatedJooqClassesMatchTheMigratedSchema(@Autowired org.jooq.DSLContext db) {
    io.zeroshift.platform.testing.GeneratedSchema.assertMatchesDatabase(
        db, io.zeroshift.platform.db.Public.PUBLIC, io.zeroshift.payment.db.Public.PUBLIC);
  }

  @Test
  @Order(1)
  void theSameKeyChargesOnceAndAnswersEveryCommandWithTheSamePayment() throws Exception {
    var order = UUID.randomUUID();
    send(kafka, new AuthorizePayment(order, new BigDecimal("68.99"), "USD", null));
    send(kafka, new AuthorizePayment(order, new BigDecimal("68.99"), "USD", null)); // new event id

    var replies = replies(Topics.PAYMENT_EVENTS, order, 2);
    assertThat(replies)
        .allSatisfy(r -> assertThat(r.payload()).isInstanceOf(PaymentAuthorized.class));
    var first = (PaymentAuthorized) replies.get(0).payload();
    assertThat(first.amount()).isEqualByComparingTo("68.99");
    assertThat(first.currency()).isEqualTo("USD");
    assertThat(((PaymentAuthorized) replies.get(1).payload()).paymentId())
        .isEqualTo(first.paymentId());
    assertThat(calls(order, "CHARGED")).isEqualTo(1);
    assertThat(gatewayRequestsWithKey(AuthorizePayment.keyFor(order))).isEqualTo(1);

    var payments = get("/payments?orderId=" + order);
    assertThat(payments.statusCode()).isEqualTo(200);
    var list = json(payments).path("data");
    assertThat(list.size()).isEqualTo(1);
    var payment = list.get(0);
    assertThat(payment.path("id").asString()).isEqualTo(first.paymentId().toString());
    assertThat(payment.path("idempotencyKey").asString()).isEqualTo(AuthorizePayment.keyFor(order));
    assertThat(payment.path("status").asString()).isEqualTo("AUTHORIZED");
    assertThat(payment.path("method").asString()).isEqualTo("CARD");
    assertThat(payment.path("amount").decimalValue()).isEqualByComparingTo("68.99");
    assertThat(payment.path("provider").asString()).isEqualTo("zeroshift-gateway");
    assertThat(payment.path("providerReference").asString()).startsWith("ch_");
    assertThat(payment.path("authorizedAt").isNull()).isFalse();

    var byId = get("/payments/" + first.paymentId());
    assertThat(byId.statusCode()).isEqualTo(200);
    assertThat(json(byId).path("status").asString()).isEqualTo("AUTHORIZED");
    var missing = get("/payments/" + UUID.randomUUID());
    assertThat(missing.statusCode()).isEqualTo(404);
    assertThat(missing.body()).contains("\"code\":\"PAYMENT_NOT_FOUND\"");
  }

  @Test
  @Order(2)
  void differentKeysForTheSameOrderAreTwoPayments() throws Exception {
    var order = UUID.randomUUID();
    send(kafka, new AuthorizePayment(order, new BigDecimal("20.00"), "EUR", "lab:" + order + ":a"));
    send(kafka, new AuthorizePayment(order, new BigDecimal("20.00"), "EUR", "lab:" + order + ":b"));

    var replies = replies(Topics.PAYMENT_EVENTS, order, 2);
    assertThat(replies)
        .extracting(r -> (PaymentAuthorized) r.payload())
        .allSatisfy(a -> assertThat(a.currency()).isEqualTo("EUR"))
        .extracting(PaymentAuthorized::paymentId)
        .doesNotHaveDuplicates();
    assertThat(calls(order, "CHARGED")).isEqualTo(2);
    assertThat(json(get("/payments?orderId=" + order)).path("data").size()).isEqualTo(2);
  }

  @Test
  @Order(3)
  void declinesOverTheLimitWithoutCallingTheGateway() throws Exception {
    var order = UUID.randomUUID();
    send(kafka, new AuthorizePayment(order, new BigDecimal("1500.00"), "USD", null));

    var reply = replies(Topics.PAYMENT_EVENTS, order, 1).getFirst();
    assertThat(((PaymentDeclined) reply.payload()).reason()).contains("exceeds");
    assertThat(calls(order, null)).isZero();
    var payment = json(get("/payments?orderId=" + order)).path("data").get(0);
    assertThat(payment.path("status").asString()).isEqualTo("DECLINED");
    assertThat(payment.path("failureReason").asString()).contains("exceeds");
  }

  @Test
  @Order(4)
  void refundsAnAuthorizedPayment() throws Exception {
    var order = UUID.randomUUID();
    send(kafka, new AuthorizePayment(order, new BigDecimal("42.50"), "USD", null));
    replies(Topics.PAYMENT_EVENTS, order, 1);
    send(kafka, new RefundPayment(order, "stock rejected"));

    var refunded = (PaymentRefunded) replies(Topics.PAYMENT_EVENTS, order, 2).get(1).payload();
    assertThat(refunded.paymentId()).isNotNull();
    assertThat(refunded.amount()).isEqualByComparingTo("42.50");
    assertThat(refunded.currency()).isEqualTo("USD");
    var payment = json(get("/payments/" + refunded.paymentId()));
    assertThat(payment.path("status").asString()).isEqualTo("REFUNDED");
    assertThat(payment.path("refundedAt").isNull()).isFalse();
  }

  @Test
  @Order(5)
  void aRefundBeforeTheAuthorizationVoidsTheOrder() throws Exception {
    var order = UUID.randomUUID();
    send(kafka, new RefundPayment(order, "timeout"));
    var refunded = (PaymentRefunded) replies(Topics.PAYMENT_EVENTS, order, 1).getFirst().payload();
    assertThat(refunded.paymentId()).isNull();
    assertThat(refunded.amount()).isZero();
    send(kafka, new AuthorizePayment(order, BigDecimal.TEN, "USD", null));

    await().atMost(WAIT).until(() -> decisions(order, "IGNORED") == 1);
    assertThat(calls(order, null)).isZero();
    var payment = json(get("/payments?orderId=" + order)).path("data").get(0);
    assertThat(payment.path("status").asString()).isEqualTo("VOIDED");
    assertThat(payment.path("idempotencyKey").asString()).isEqualTo(AuthorizePayment.keyFor(order));
  }

  @Test
  @Order(6)
  void aDownGatewayIsRetriedThenTheBreakerOpensAndTheCommandIsDeadLettered() throws Exception {
    gateway.set("down");
    var order = UUID.randomUUID();
    send(kafka, new AuthorizePayment(order, BigDecimal.TEN, "USD", null));

    var parked =
        CommerceStack.read(Topics.deadLetter(Topics.PAYMENT_COMMANDS), forKey(order), 1, WAIT);
    assertThat(parked).hasSize(1);
    assertThat(calls(order, "HTTP_503")).isGreaterThanOrEqualTo(3); // in-process retries
    assertThat(calls(order, "REJECTED_BY_BREAKER")).isGreaterThanOrEqualTo(1); // breaker opened
    assertThat(decisions(order, "RETRY_SCHEDULED")).isEqualTo(4); // Kafka redeliveries
    assertThat(decisions(order, "DEAD_LETTERED")).isEqualTo(1);
    // The claim outlived every rolled-back delivery: one payment, still waiting for an answer.
    var payments = json(get("/payments?orderId=" + order)).path("data");
    assertThat(payments.size()).isEqualTo(1);
    assertThat(payments.get(0).path("status").asString()).isEqualTo("PENDING");

    gateway.set("healthy");
    gateway.resetBreaker();
    var next = UUID.randomUUID();
    send(kafka, new AuthorizePayment(next, BigDecimal.TEN, "USD", null));
    assertThat(replies(Topics.PAYMENT_EVENTS, next, 1).getFirst().payload())
        .isInstanceOf(PaymentAuthorized.class);
  }

  private static String gatewayUrl() {
    return "http://" + GATEWAY.getHost() + ":" + GATEWAY.getMappedPort(8080);
  }

  /** Charges the gateway received with this Idempotency-Key header (WireMock's request journal). */
  private static int gatewayRequestsWithKey(String key) throws Exception {
    var body =
        MessageCodec.json()
            .writeValueAsString(
                Map.of(
                    "method", "POST",
                    "urlPath", "/charges",
                    "headers", Map.of("Idempotency-Key", Map.of("equalTo", key))));
    var response =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build()
            .send(
                HttpRequest.newBuilder(URI.create(gatewayUrl() + "/__admin/requests/count"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
    return MessageCodec.json().readTree(response.body()).path("count").asInt();
  }

  private HttpResponse<String> get(String path) throws Exception {
    return HttpClient.newHttpClient()
        .send(
            HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build(),
            HttpResponse.BodyHandlers.ofString());
  }

  private static JsonNode json(HttpResponse<String> response) {
    return MessageCodec.json().readTree(response.body());
  }

  private int calls(UUID order, String outcome) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM gateway_call WHERE order_id=? AND (?::text IS NULL OR outcome=?)",
        Integer.class,
        order,
        outcome,
        outcome);
  }

  private int decisions(UUID order, String decision) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM consumer_decision WHERE order_id=? AND decision=?",
        Integer.class,
        order.toString(),
        decision);
  }
}
