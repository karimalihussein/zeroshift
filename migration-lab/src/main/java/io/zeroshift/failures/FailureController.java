package io.zeroshift.failures;

import io.zeroshift.eventlab.EventLabSettings;
import io.zeroshift.kafkalab.KafkaLabRuns;
import io.zeroshift.platform.web.ApiException;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import tools.jackson.databind.node.ObjectNode;

/** Phase 5: the failure lab page, its four experiments, their inspectors and recovery actions. */
@Controller
@Validated
public class FailureController {
  private final FailureLabs labs;
  private final TwoPhaseLab twoPhase;
  private final FailureDb db;
  private final FailureLabSettings settings;
  private final EventLabSettings events;
  private final KafkaLabRuns runs;

  public FailureController(
      FailureLabs labs,
      TwoPhaseLab twoPhase,
      FailureDb db,
      FailureLabSettings settings,
      EventLabSettings events,
      KafkaLabRuns runs) {
    this.labs = labs;
    this.twoPhase = twoPhase;
    this.db = db;
    this.settings = settings;
    this.events = events;
    this.runs = runs;
  }

  @GetMapping("/failures")
  public String page(Model model) {
    model.addAttribute("grafanaUrl", events.grafanaUrl());
    return "failures";
  }

  /** Whether each piece of the lab's infrastructure answers, as it answered. */
  @GetMapping("/api/failures/status")
  @ResponseBody
  public Map<String, Object> status() {
    var out = new LinkedHashMap<String, Object>();
    out.put("failurePostgres", postgres());
    out.put("kafka", kafka(events.kafkaBootstrap(), null));
    out.put(
        "kafkaSecure",
        settings.secureKafkaConfigured()
            ? kafka(settings.secureKafka(), settings.password("admin"))
            : Map.of(
                "up",
                false,
                "detail",
                "not configured: docker compose --profile failure-lab up -d"));
    return out;
  }

  @GetMapping("/api/failures/labs")
  @ResponseBody
  public List<FailureLabs.Info> labs() {
    return labs.all();
  }

  /** Runs stage {@code n} (0-based) of a lab; 0 starts it over. */
  @PostMapping("/api/failures/labs/{id}/stages/{n}")
  @ResponseBody
  public FailureLabs.Run stage(@PathVariable String id, @PathVariable @Min(0) @Max(4) int n) {
    return labs.stage(id, n);
  }

  @GetMapping("/api/failures/labs/{id}/state")
  @ResponseBody
  public ObjectNode state(@PathVariable String id) {
    return unavailable(() -> labs.state(id));
  }

  @PostMapping("/api/failures/labs/{id}/reset")
  @ResponseBody
  public ObjectNode reset(@PathVariable String id) {
    return unavailable(() -> labs.reset(id));
  }

  /** Starts a 2PC checkout whose coordinator is killed at {@code crashAt}, for a manual drill. */
  @PostMapping("/api/failures/two-phase/in-doubt")
  @ResponseBody
  public Map<String, Object> inDoubt(@RequestParam(defaultValue = "after-prepare") String crashAt) {
    return unavailable(() -> labs.exclusive(twoPhase.id(), () -> twoPhase.startInDoubt(crashAt)));
  }

  /** COMMIT PREPARED or ROLLBACK PREPARED one in-doubt branch, by hand. */
  @PostMapping("/api/failures/two-phase/prepared/{gid}/{action}")
  @ResponseBody
  public Map<String, Object> resolve(@PathVariable String gid, @PathVariable String action) {
    return unavailable(
        () ->
            labs.exclusive(
                twoPhase.id(),
                () ->
                    twoPhase.resolve(gid, action.toUpperCase(java.util.Locale.ROOT), "operator")));
  }

  /** The recovery procedure: each in-doubt branch resolved as the coordinator's log says. */
  @PostMapping("/api/failures/two-phase/recover")
  @ResponseBody
  public List<Map<String, Object>> recover() {
    return unavailable(() -> labs.exclusive(twoPhase.id(), twoPhase::recoverInDoubt));
  }

  @GetMapping("/api/failures/runs")
  @ResponseBody
  public List<KafkaLabRuns.Run> runs(
      @RequestParam(defaultValue = "10") @Min(1) @Max(50) int limit) {
    return runs.recent(FailureLabs.LAB, limit);
  }

  private Map<String, Object> postgres() {
    if (!settings.postgresConfigured())
      return Map.of(
          "up", false, "detail", "not configured: docker compose --profile failure-lab up -d");
    try (var c = db.connect(null)) {
      var row =
          FailureDb.one(
              c,
              "SELECT current_setting('server_version') AS version, current_setting('max_prepared_transactions') AS max_prepared_transactions");
      var out = new LinkedHashMap<String, Object>(row);
      out.put("up", true);
      return out;
    } catch (SQLException e) {
      return Map.of("up", false, "detail", String.valueOf(e.getMessage()));
    }
  }

  private static Map<String, Object> kafka(String bootstrap, String adminPassword) {
    var p = new Properties();
    p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "3000");
    p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "2500");
    if (adminPassword != null) {
      p.put("security.protocol", "SASL_PLAINTEXT");
      p.put("sasl.mechanism", "PLAIN");
      p.put(
          "sasl.jaas.config",
          "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\""
              + adminPassword
              + "\";");
    }
    try (var admin = Admin.create(p)) {
      var cluster = admin.describeCluster();
      return Map.of(
          "up",
          true,
          "bootstrap",
          bootstrap,
          "brokers",
          cluster.nodes().get(3, TimeUnit.SECONDS).size(),
          "authentication",
          adminPassword == null ? "none (PLAINTEXT)" : "SASL/PLAIN as admin");
    } catch (Exception e) {
      return Map.of(
          "up",
          false,
          "bootstrap",
          bootstrap,
          "detail",
          String.valueOf(SecurityDecision.root(e).getMessage()));
    }
  }

  /** Infrastructure that is down becomes 503 FAILURE_LAB_UNAVAILABLE; bad input 400. */
  private static <T> T unavailable(java.util.concurrent.Callable<T> action) {
    try {
      return action.call();
    } catch (ApiException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_FAILURE_ACTION", e.getMessage());
    } catch (Exception e) {
      throw new ApiException(
          HttpStatus.SERVICE_UNAVAILABLE,
          "FAILURE_LAB_UNAVAILABLE",
          e.getClass().getSimpleName() + ": " + e.getMessage());
    }
  }
}
