package io.zeroshift.order.infrastructure;

import static io.zeroshift.order.db.Tables.LEASE;
import static org.jooq.impl.DSL.when;

import io.zeroshift.platform.PostgresClock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.OptionalLong;
import org.jooq.DSLContext;

/**
 * A distributed lock across service replicas, held as a lease in the shared database. One statement
 * both acquires and renews: it succeeds when nobody holds the lease, when this owner already holds
 * it, or when the holder let it expire (crashed, paused, partitioned). A new owner gets the next
 * fencing token. All times are PostgreSQL's, so the replicas' clocks never matter.
 */
public final class PostgresLease {
  private final DSLContext db;
  private final String owner;

  public PostgresLease(DSLContext db, String owner) {
    this.db = db;
    this.owner = owner;
  }

  public String owner() {
    return owner;
  }

  /** The fencing token if this instance now holds {@code name} for {@code ttl}; empty otherwise. */
  public OptionalLong acquire(String name, Duration ttl) {
    var expiry = PostgresClock.nowPlus(ttl);
    return db.insertInto(LEASE)
        .set(LEASE.NAME, name)
        .set(LEASE.OWNER, owner)
        .set(LEASE.TOKEN, 1L)
        .set(LEASE.EXPIRES_AT, expiry)
        .onConflict(LEASE.NAME)
        .doUpdate()
        // Renewal keeps the token; a takeover increments it. The token only ever grows.
        .set(LEASE.TOKEN, when(LEASE.OWNER.eq(owner), LEASE.TOKEN).otherwise(LEASE.TOKEN.plus(1)))
        .set(
            LEASE.ACQUIRED_AT,
            when(LEASE.OWNER.eq(owner), LEASE.ACQUIRED_AT).otherwise(PostgresClock.NOW))
        .set(LEASE.OWNER, owner)
        .set(LEASE.EXPIRES_AT, expiry)
        // Anyone else's live lease is left alone: the upsert then updates nothing.
        .where(LEASE.OWNER.eq(owner).or(LEASE.EXPIRES_AT.lt(PostgresClock.NOW)))
        .returning(LEASE.TOKEN)
        .fetchOptional(LEASE.TOKEN)
        .map(OptionalLong::of)
        .orElse(OptionalLong.empty());
  }

  /**
   * Fencing check, to call inside the transaction doing the guarded work: true while {@code token}
   * is still the current one. The share lock holds until that transaction ends, so a takeover
   * (which updates the row) waits for the guarded work to commit.
   */
  public boolean fence(String name, long token) {
    return db.fetchExists(
        db.selectOne()
            .from(LEASE)
            .where(LEASE.NAME.eq(name).and(LEASE.OWNER.eq(owner)).and(LEASE.TOKEN.eq(token)))
            .forShare());
  }

  /** The lease as it stands; owner and times are null when nobody has ever held it. */
  public record View(
      String name,
      String owner,
      Long token,
      OffsetDateTime acquiredAt,
      OffsetDateTime expiresAt,
      boolean held) {}

  public View describe(String name) {
    return db.select(
            LEASE.NAME,
            LEASE.OWNER,
            LEASE.TOKEN,
            LEASE.ACQUIRED_AT,
            LEASE.EXPIRES_AT,
            LEASE.EXPIRES_AT.gt(PostgresClock.NOW))
        .from(LEASE)
        .where(LEASE.NAME.eq(name))
        .fetchOptional(
            r -> new View(r.value1(), r.value2(), r.value3(), r.value4(), r.value5(), r.value6()))
        .orElse(new View(name, null, null, null, null, false));
  }
}
