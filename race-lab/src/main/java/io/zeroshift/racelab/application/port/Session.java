package io.zeroshift.racelab.application.port;

import io.zeroshift.racelab.domain.Isolation;
import java.util.List;
import java.util.Map;

/**
 * One request's own database connection. Every statement runs for real; a failure surfaces as a
 * {@link DbFailure} carrying PostgreSQL's SQLSTATE, message and detail.
 */
public interface Session extends AutoCloseable {
  /** {@code pg_backend_pid()}: how PostgreSQL's lock tables name this connection. */
  int pid();

  /** Starts a transaction at the isolation level (autocommit off, {@code SET TRANSACTION}). */
  void begin(Isolation isolation);

  /** {@code pg_current_xact_id()}: assigns and returns this transaction's id. */
  long txId();

  List<Map<String, Object>> query(String sql, List<Object> params);

  int update(String sql, List<Object> params);

  void commit();

  void rollback();

  @Override
  void close();

  /** A statement PostgreSQL refused or aborted. */
  final class DbFailure extends RuntimeException {
    public enum Kind {
      /** 40001 serialization_failure. */
      SERIALIZATION,
      /** 40P01 deadlock_detected. */
      DEADLOCK,
      /** 55P03 lock_not_available (NOWAIT, lock_timeout). */
      LOCK_NOT_AVAILABLE,
      /** 57014 query_canceled. */
      CANCELED,
      OTHER
    }

    private final Kind kind;
    private final String sqlState;
    private final String detail;

    public DbFailure(Kind kind, String sqlState, String message, String detail, Throwable cause) {
      super(message, cause);
      this.kind = kind;
      this.sqlState = sqlState;
      this.detail = detail;
    }

    public Kind kind() {
      return kind;
    }

    public String sqlState() {
      return sqlState;
    }

    /** PostgreSQL's DETAIL line: for a deadlock, which process waited for which. */
    public String detail() {
      return detail;
    }
  }
}
