package io.zeroshift.failures;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;

/**
 * The two participants and the coordinator's log, each its own database on the failure lab's
 * PostgreSQL: fl_payments (accounts, payments), fl_inventory (stock, reservations) and
 * fl_coordinator (the protocol log). Two identical account/SKU pairs: one for the 2PC run, one for
 * the saga run, so the in-doubt 2PC transaction's locks never touch what the saga run measures.
 */
final class TwoPhaseSchema {
  static final String PAYMENTS = "fl_payments";
  static final String INVENTORY = "fl_inventory";
  static final String COORDINATOR = "fl_coordinator";
  static final List<String> DATABASES = List.of(PAYMENTS, INVENTORY, COORDINATOR);

  static final String ACCOUNT_2PC = "acct-2pc";
  static final String ACCOUNT_SAGA = "acct-saga";
  static final String SKU_2PC = "SKU-2PC-WIDGET";
  static final String SKU_SAGA = "SKU-SAGA-WIDGET";

  /** Never touched by an order: a checkout of it shows that locks are per row. */
  static final String SKU_OTHER = "SKU-OTHER";

  static final BigDecimal BALANCE = new BigDecimal("1000.00");
  static final int STOCK = 100;
  static final BigDecimal AMOUNT = new BigDecimal("49.90");
  static final int QUANTITY = 1;

  private TwoPhaseSchema() {}

  /** Drops and recreates the three databases with their tables and seed rows. */
  static void recreate(FailureDb db) throws SQLException {
    for (var name : DATABASES) db.drop(name);
    for (var name : DATABASES) db.create(name);
    db.execute(
        PAYMENTS,
        "CREATE TABLE account (id text PRIMARY KEY, balance numeric(12,2) NOT NULL CHECK (balance >= 0))",
        "CREATE TABLE payment (order_id text PRIMARY KEY, account_id text NOT NULL REFERENCES account,"
            + " amount numeric(12,2) NOT NULL, status text NOT NULL, at timestamptz NOT NULL DEFAULT now())",
        "INSERT INTO account VALUES ('"
            + ACCOUNT_2PC
            + "', "
            + BALANCE
            + "), ('"
            + ACCOUNT_SAGA
            + "', "
            + BALANCE
            + ")");
    db.execute(
        INVENTORY,
        "CREATE TABLE stock (sku text PRIMARY KEY, on_hand int NOT NULL CHECK (on_hand >= 0))",
        "CREATE TABLE reservation (order_id text PRIMARY KEY, sku text NOT NULL REFERENCES stock,"
            + " quantity int NOT NULL, status text NOT NULL, at timestamptz NOT NULL DEFAULT now())",
        // Rewritten over and over while a transaction is in doubt: VACUUM shows what it cannot
        // remove.
        "CREATE TABLE vacuum_probe (id int PRIMARY KEY, n int NOT NULL)",
        "INSERT INTO vacuum_probe VALUES (1, 0)",
        "INSERT INTO stock VALUES ('"
            + SKU_2PC
            + "', "
            + STOCK
            + "), ('"
            + SKU_SAGA
            + "', "
            + STOCK
            + "), ('"
            + SKU_OTHER
            + "', "
            + STOCK
            + ")");
    db.execute(
        COORDINATOR,
        "CREATE TABLE coordinator_log (gid text PRIMARY KEY, protocol text NOT NULL, order_id text NOT NULL,"
            + " state text NOT NULL, decision text, deadline timestamptz, started_at timestamptz NOT NULL DEFAULT now(),"
            + " updated_at timestamptz NOT NULL DEFAULT now())",
        "CREATE TABLE coordinator_event (id bigserial PRIMARY KEY, gid text NOT NULL, at timestamptz NOT NULL"
            + " DEFAULT clock_timestamp(), actor text NOT NULL, action text NOT NULL, detail text)");
  }
}
