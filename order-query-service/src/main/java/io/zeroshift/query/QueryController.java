package io.zeroshift.query;

import java.util.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** The query API: reads only, straight from the read models. */
@RestController
public class QueryController {
  private final JdbcTemplate jdbc;
  private final ProjectionRebuild rebuild;

  public QueryController(JdbcTemplate jdbc, ProjectionRebuild rebuild) {
    this.jdbc = jdbc;
    this.rebuild = rebuild;
  }

  // PostgreSQL renders the rows as JSON: arrays and jsonb columns arrive as real JSON values.
  @GetMapping(value = "/orders", produces = MediaType.APPLICATION_JSON_VALUE)
  public String orders(@RequestParam(defaultValue = "50") int limit) {
    return jdbc.queryForObject(
        "SELECT COALESCE(json_agg(v ORDER BY v.placed_at DESC),'[]')::text FROM"
            + " (SELECT * FROM order_view ORDER BY placed_at DESC LIMIT ?) v",
        String.class,
        Math.min(limit, 500));
  }

  @GetMapping(value = "/orders/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
  public String order(@PathVariable UUID id) {
    return jdbc
        .queryForList(
            "SELECT row_to_json(v)::text FROM order_view v WHERE order_id=?", String.class, id)
        .stream()
        .findFirst()
        .orElseThrow(
            () -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Not projected (yet): " + id));
  }

  @GetMapping(value = "/customers", produces = MediaType.APPLICATION_JSON_VALUE)
  public String customers() {
    return jdbc.queryForObject(
        "SELECT COALESCE(json_agg(c ORDER BY c.shipped_value DESC, c.customer_id),'[]')::text"
            + " FROM customer_summary c",
        String.class);
  }

  @GetMapping("/lab/projection")
  public Map<String, Object> projection() {
    return jdbc.queryForMap(
        "SELECT (SELECT COUNT(*) FROM order_view) AS orders,(SELECT COUNT(*) FROM customer_summary) AS customers,"
            + "(SELECT MAX(projected_at) FROM order_view) AS last_projected_at");
  }

  @PostMapping("/lab/projection/rebuild")
  public ProjectionRebuild.Started rebuild() throws Exception {
    return rebuild.rebuild();
  }
}
