package io.zeroshift.resilience;

import static org.assertj.core.api.Assertions.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * The chaos client against a real Toxiproxy in front of a real PostgreSQL: every fault the lab
 * offers changes what a JDBC client actually experiences, and healing undoes it.
 */
class ToxiproxyIT {
  static final Network NETWORK = Network.newNetwork();
  static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:17-alpine").withNetwork(NETWORK).withNetworkAliases("db");
  static final GenericContainer<?> TOXIPROXY =
      new GenericContainer<>("ghcr.io/shopify/toxiproxy:2.12.0")
          .withNetwork(NETWORK)
          .withExposedPorts(8474, 15432)
          .waitingFor(Wait.forHttp("/version").forPort(8474));

  static Toxiproxy chaos;

  @BeforeAll
  static void start() throws Exception {
    POSTGRES.start();
    TOXIPROXY.start();
    var api = "http://" + TOXIPROXY.getHost() + ":" + TOXIPROXY.getMappedPort(8474);
    // The Compose stack declares its proxies in infra/toxiproxy/toxiproxy.json; here, one of them.
    var created =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(URI.create(api + "/proxies"))
                    .POST(
                        HttpRequest.BodyPublishers.ofString(
                            "{\"name\":\"order-db\",\"listen\":\"0.0.0.0:15432\",\"upstream\":\"db:5432\"}"))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
    assertThat(created.statusCode()).isEqualTo(201);
    chaos = new Toxiproxy(new ResilienceSettings(api));
  }

  @AfterAll
  static void stop() {
    TOXIPROXY.stop();
    POSTGRES.stop();
  }

  @BeforeEach
  void heal() {
    chaos.heal("order-db");
  }

  @Test
  void latencyDelaysEveryAnswer() throws Exception {
    long direct = query();
    chaos.inject("order-db", Toxiproxy.Fault.LATENCY, 400, 0);
    assertThat(chaos.links())
        .singleElement()
        .satisfies(
            l -> {
              assertThat(l.healthy()).isFalse();
              assertThat(l.toxics()).extracting(Toxiproxy.Toxic::type).containsExactly("latency");
            });
    assertThat(query()).isGreaterThanOrEqualTo(direct + 400);
  }

  @Test
  void aPartitionHangsTheClientUntilItsOwnTimeout() {
    chaos.inject("order-db", Toxiproxy.Fault.PARTITION, 0, 0);
    long started = System.nanoTime();
    assertThatThrownBy(this::query).isInstanceOf(SQLException.class);
    assertThat((System.nanoTime() - started) / 1_000_000)
        .as("no error until the client gives up")
        .isGreaterThanOrEqualTo(1900);
  }

  @Test
  void downRefusesConnectionsAndHealRestoresThem() throws Exception {
    chaos.inject("order-db", Toxiproxy.Fault.DOWN, 0, 0);
    assertThat(chaos.links().getFirst().enabled()).isFalse();
    assertThatThrownBy(this::query).isInstanceOf(SQLException.class);

    chaos.heal("order-db");
    assertThat(chaos.links().getFirst().healthy()).isTrue();
    assertThat(query()).isLessThan(2000);
  }

  @Test
  void resetBreaksConnectionsAsSoonAsDataFlows() {
    chaos.inject("order-db", Toxiproxy.Fault.RESET, 0, 0);
    assertThatThrownBy(this::query).isInstanceOf(SQLException.class);
  }

  @Test
  void onlyTheLabsOwnLinksCanBeBroken() {
    assertThatThrownBy(() -> chaos.inject("somebody-elses-db", Toxiproxy.Fault.DOWN, 0, 0))
        .hasMessageContaining("No chaos link");
  }

  /** Connects through the proxy and runs one query; returns how long it took, in ms. */
  private long query() throws SQLException {
    var url =
        "jdbc:postgresql://"
            + TOXIPROXY.getHost()
            + ":"
            + TOXIPROXY.getMappedPort(15432)
            + "/"
            + POSTGRES.getDatabaseName();
    var props = new Properties();
    props.setProperty("user", POSTGRES.getUsername());
    props.setProperty("password", POSTGRES.getPassword());
    props.setProperty("connectTimeout", "2");
    props.setProperty("socketTimeout", "2");
    long started = System.nanoTime();
    try (var c = DriverManager.getConnection(url, props);
        var rs = c.createStatement().executeQuery("SELECT 1")) {
      rs.next();
    }
    return (System.nanoTime() - started) / 1_000_000;
  }
}
