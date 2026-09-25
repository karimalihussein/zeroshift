package io.zeroshift.payment;

import static io.zeroshift.platform.testing.ServiceTestSupport.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import io.zeroshift.contracts.PaymentCommand.*;
import io.zeroshift.contracts.PaymentEvent.*;
import io.zeroshift.contracts.Topics;
import io.zeroshift.payment.infrastructure.GatewayControl;
import io.zeroshift.platform.testing.CommerceStack;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

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
    registry.add(
        "payment.gateway-url",
        () -> "http://" + GATEWAY.getHost() + ":" + GATEWAY.getMappedPort(8080));
  }

  @Autowired KafkaTemplate<String, String> kafka;
  @Autowired JdbcTemplate jdbc;
  @Autowired GatewayControl gateway;

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
  void authorizesThroughTheGatewayOnceEvenIfTheCommandIsSentTwice() {
    var order = UUID.randomUUID();
    send(kafka, new AuthorizePayment(order, new BigDecimal("68.99"), "USD"));
    send(kafka, new AuthorizePayment(order, new BigDecimal("68.99"), "USD")); // new event id

    var replies = replies(Topics.PAYMENT_EVENTS, order, 2);
    assertThat(replies)
        .allSatisfy(r -> assertThat(r.payload()).isInstanceOf(PaymentAuthorized.class));
    assertThat(((PaymentAuthorized) replies.get(0).payload()).paymentId())
        .isEqualTo(((PaymentAuthorized) replies.get(1).payload()).paymentId());
    assertThat(calls(order, "CHARGED")).isEqualTo(1);
  }

  @Test
  @Order(2)
  void declinesOverTheLimitWithoutCallingTheGateway() {
    var order = UUID.randomUUID();
    send(kafka, new AuthorizePayment(order, new BigDecimal("1500.00"), "USD"));

    var reply = replies(Topics.PAYMENT_EVENTS, order, 1).getFirst();
    assertThat(((PaymentDeclined) reply.payload()).reason()).contains("exceeds");
    assertThat(calls(order, null)).isZero();
  }

  @Test
  @Order(3)
  void aRefundBeforeTheAuthorizationVoidsTheOrder() {
    var order = UUID.randomUUID();
    send(kafka, new RefundPayment(order, "timeout"));
    replies(Topics.PAYMENT_EVENTS, order, 1);
    send(kafka, new AuthorizePayment(order, BigDecimal.TEN, "USD"));

    await().atMost(WAIT).until(() -> decisions(order, "IGNORED") == 1);
    assertThat(calls(order, null)).isZero();
  }

  @Test
  @Order(4)
  void aDownGatewayIsRetriedThenTheBreakerOpensAndTheCommandIsDeadLettered() {
    gateway.set("down");
    var order = UUID.randomUUID();
    send(kafka, new AuthorizePayment(order, BigDecimal.TEN, "USD"));

    var parked =
        CommerceStack.read(Topics.deadLetter(Topics.PAYMENT_COMMANDS), forKey(order), 1, WAIT);
    assertThat(parked).hasSize(1);
    assertThat(calls(order, "HTTP_503")).isGreaterThanOrEqualTo(3); // in-process retries
    assertThat(calls(order, "REJECTED_BY_BREAKER")).isGreaterThanOrEqualTo(1); // breaker opened
    assertThat(decisions(order, "RETRY_SCHEDULED")).isEqualTo(4); // Kafka redeliveries
    assertThat(decisions(order, "DEAD_LETTERED")).isEqualTo(1);

    gateway.set("healthy");
    gateway.resetBreaker();
    var next = UUID.randomUUID();
    send(kafka, new AuthorizePayment(next, BigDecimal.TEN, "USD"));
    assertThat(replies(Topics.PAYMENT_EVENTS, next, 1).getFirst().payload())
        .isInstanceOf(PaymentAuthorized.class);
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
