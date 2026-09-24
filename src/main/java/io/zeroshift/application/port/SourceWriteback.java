package io.zeroshift.application.port;

import io.zeroshift.domain.*;
import java.util.List;

/** Reverse-sync writes into SQL Server, the only writer allowed past its fence after cutover. */
public interface SourceWriteback {
  /**
   * Applies net images in one SQL Server transaction: upserts with their PostgreSQL keys, deletes
   * by key. Before writing, locks the affected keys and checks Change Tracking since {@code
   * baselineVersion}; if any key was changed outside reverse sync, nothing is written and those
   * conflicts are returned.
   */
  List<ReverseConflict> apply(Table table, List<Change> changes, long baselineVersion);

  /**
   * Moves each identity seed past both SQL Server's highest key and the highest key PostgreSQL ever
   * issued, so no key handed out after cutover (even one since deleted) is reused.
   */
  void synchronizeIdentities(java.util.Map<Table, Long> issuedByPostgres);
}
