package io.zeroshift.application.port;

import io.zeroshift.domain.*;
import java.util.List;

public interface SourceDatabase {
  record Boundary(long version, long customers, long orders, long count) {}

  Boundary boundary();

  List<Row> read(Table table, long afterId, long upperId, int limit);

  Capture capture(long afterVersion);

  void freeze(boolean frozen);

  void seed(int rows);

  void reset();

  TrafficOperationOutcome writeTraffic(TrafficOperation operation);

  long count(Table table);

  /** One SQL Server snapshot transaction owns the version and all paged change reads. */
  interface Capture extends AutoCloseable {
    long version();

    List<Change> read(Table table, long afterId, int limit);

    long pending();

    /**
     * Keys changed after this capture's baseline by anything other than ZeroShift's reverse sync.
     * After cutover SQL Server is fenced, so any such key is a conflict a rollback must not
     * overwrite.
     */
    List<Long> outOfBand(Table table);

    @Override
    void close();
  }
}
