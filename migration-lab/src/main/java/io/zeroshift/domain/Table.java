package io.zeroshift.domain;

public enum Table {
  CUSTOMERS("customers"),
  ORDERS("orders");
  private final String sqlName;

  Table(String sqlName) {
    this.sqlName = sqlName;
  }

  public String sqlName() {
    return sqlName;
  }
}
