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
              fingerprint(after -> source.read(table, after, Long.MAX_VALUE, batchSize)),
              fingerprint(after -> store.read(table, after, batchSize))));
    return ValidationResult.of(results);
  }

  private ValidationResult.Fingerprint fingerprint(LongFunction<List<Row>> read) {
    var digest = new RowFingerprint();
    long after = 0;
    while (true) {
      var rows = read.apply(after);
      if (rows.isEmpty()) return digest.finish();
      rows.forEach(digest::add);
      after = rows.getLast().id();
    }
  }
}
