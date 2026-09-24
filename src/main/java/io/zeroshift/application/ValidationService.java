package io.zeroshift.application;

import io.zeroshift.application.port.*;
import io.zeroshift.domain.*;
import java.util.*;
import java.util.function.LongFunction;

public final class ValidationService {
  private final SourceDatabase source;
  private final MigrationStore store;
  private final int batchSize;

  public ValidationService(SourceDatabase source, MigrationStore store, int batchSize) {
    this.source = source;
    this.store = store;
    this.batchSize = batchSize;
  }

  /** Call only behind the source write fence and target routing lock. */
  public ValidationResult validate() {
    var results = new ArrayList<ValidationResult.TableResult>();
    for (var table : Table.values())
      results.add(
          new ValidationResult.TableResult(
              table,
              fingerprint(after -> source.read(table, after, Long.MAX_VALUE, batchSize), Set.of()),
              fingerprint(after -> store.read(table, after, batchSize), Set.of())));
    return ValidationResult.of(results);
  }

  /**
   * Rollback check while PostgreSQL still takes writes: compares every row except keys whose
   * captured change is not yet replayed, reading PostgreSQL from one snapshot. SQL Server is fenced
   * and only reverse sync, which runs on the caller's thread, writes to it, so it is stable.
   */
  public ValidationResult validateSettled(MigrationStore.ReverseCapture capture) {
    var results = new ArrayList<ValidationResult.TableResult>();
    for (var table : Table.values()) {
      var pending = capture.pendingKeys(table);
      results.add(
          new ValidationResult.TableResult(
              table,
              fingerprint(after -> source.read(table, after, Long.MAX_VALUE, batchSize), pending),
              fingerprint(after -> capture.rows(table, after, batchSize), pending)));
    }
    return ValidationResult.of(results);
  }

  private ValidationResult.Fingerprint fingerprint(LongFunction<List<Row>> read, Set<Long> skip) {
    var digest = new RowFingerprint();
    long after = 0;
    while (true) {
      var rows = read.apply(after);
      if (rows.isEmpty()) return digest.finish();
      for (var row : rows) if (!skip.contains(row.id())) digest.add(row);
      after = rows.getLast().id();
    }
  }
}
