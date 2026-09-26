package io.zeroshift.racelab.domain;

import java.sql.Connection;

/** PostgreSQL's transaction isolation levels (READ UNCOMMITTED behaves as READ COMMITTED). */
public enum Isolation {
  READ_COMMITTED("READ COMMITTED", Connection.TRANSACTION_READ_COMMITTED),
  REPEATABLE_READ("REPEATABLE READ", Connection.TRANSACTION_REPEATABLE_READ),
  SERIALIZABLE("SERIALIZABLE", Connection.TRANSACTION_SERIALIZABLE);

  private final String sql;
  private final int jdbc;

  Isolation(String sql, int jdbc) {
    this.sql = sql;
    this.jdbc = jdbc;
  }

  /** As written in {@code SET TRANSACTION ISOLATION LEVEL …}. */
  public String sql() {
    return sql;
  }

  public int jdbc() {
    return jdbc;
  }
}
