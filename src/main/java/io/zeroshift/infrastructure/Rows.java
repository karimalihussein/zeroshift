package io.zeroshift.infrastructure;

import io.zeroshift.domain.*;
import java.sql.ResultSet;
import java.sql.SQLException;

final class Rows {
  private Rows() {}

  static String columns(Table table) {
    return table == Table.CUSTOMERS ? "id,name,email,active" : "id,customer_id,amount,status";
  }

  static Row read(Table table, ResultSet rs) throws SQLException {
    return switch (table) {
      case CUSTOMERS ->
          new Row.Customer(
              rs.getLong("id"),
              rs.getString("name"),
              rs.getString("email"),
              rs.getBoolean("active"));
      case ORDERS ->
          new Row.Order(
              rs.getLong("id"),
              rs.getLong("customer_id"),
              rs.getBigDecimal("amount"),
              rs.getString("status"));
    };
  }
}
