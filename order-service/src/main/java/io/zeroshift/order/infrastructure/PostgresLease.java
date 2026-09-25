package io.zeroshift.order.infrastructure;

import java.time.Duration;
import java.util.Map;
import java.util.OptionalLong;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * A distributed lock across service replicas, held as a lease in the shared database. One statement
 * both acquires and renews: it succeeds when nobody holds the lease, when this owner already holds
 * it, or when the holder let it expire (crashed, paused, partitioned). A new owner gets the next
 * fencing token.
 */
public final class PostgresLease {
  private final JdbcTemplate jdbc;
  private final String owner;

  public PostgresLease(JdbcTemplate jdbc, String owner) {
    this.jdbc = jdbc;
    this.owner = owner;
  }

  public String owner() {
    return owner;
  }

  /** The fencing token if this instance now holds {@code name} for {@code ttl}; empty otherwise. */
  public OptionalLong acquire(String name, Duration ttl) {
    return jdbc
        .query(
            "INSERT INTO lease(name,owner,token,expires_at) VALUES(?,?,1,clock_timestamp()+?::interval)"
                + " ON CONFLICT(name) DO UPDATE SET"
                + " token=lease.token+CASE WHEN lease.owner=EXCLUDED.owner THEN 0 ELSE 1 END,"
                + " acquired_at=CASE WHEN lease.owner=EXCLUDED.owner THEN lease.acquired_at ELSE clock_timestamp() END,"
                + " owner=EXCLUDED.owner, expires_at=EXCLUDED.expires_at"
                + " WHERE lease.owner=EXCLUDED.owner OR lease.expires_at<clock_timestamp() RETURNING token",
            (r, n) -> r.getLong(1),
            name,
            owner,
            ttl.toMillis() + " milliseconds")
        .stream()
        .mapToLong(Long::longValue)
        .findFirst();
  }

  /**
   * Fencing check, to call inside the transaction doing the guarded work: true while {@code token}
   * is still the current one. The share lock holds until that transaction ends, so a takeover
   * (which updates the row) waits for the guarded work to commit.
   */
  public boolean fence(String name, long token) {
    return !jdbc.queryForList(
            "SELECT 1 FROM lease WHERE name=? AND owner=? AND token=? FOR SHARE",
            name,
            owner,
            token)
        .isEmpty();
  }

  public Map<String, Object> describe(String name) {
    return jdbc
        .queryForList(
            "SELECT name,owner,token,acquired_at,expires_at,expires_at>clock_timestamp() AS held"
                + " FROM lease WHERE name=?",
            name)
        .stream()
        .findFirst()
        .orElse(Map.of("name", name, "held", false));
  }
}
