package io.zeroshift.domain;

/** A SQL Server row changed after cutover by something other than ZeroShift's reverse sync. */
public record ReverseConflict(Table table, long id) {
  @Override
  public String toString() {
    return table.sqlName() + " #" + id;
  }
}
