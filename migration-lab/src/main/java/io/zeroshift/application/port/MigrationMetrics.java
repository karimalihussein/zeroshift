package io.zeroshift.application.port;

import io.zeroshift.domain.Table;
import java.time.Duration;

/**
 * Timings only the engine can observe. Counts and progress are durable state; adapters read them
 * from the store rather than keeping a second tally here.
 */
public interface MigrationMetrics {
  record BatchTiming(long count, double meanMillis, double maxMillis) {}

  /** Called once a snapshot batch's rows and checkpoint are written in its transaction. */
  void batchCopied(Table table, Duration elapsed);

  void cutoverFinished(Duration elapsed);

  BatchTiming batchTiming();
}
