package io.zeroshift.query;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Read-your-writes on an eventually consistent read model. A writer gets its order's version back
 * (a consistency token); reading with that token waits, bounded, until the projection has applied
 * at least that version, and otherwise says how far behind it is instead of serving the stale row.
 */
public final class ReadYourWrites {
  /** The longest a read may wait for the projection. */
  public static final Duration MAX_WAIT = Duration.ofSeconds(5);

  private static final Duration POLL = Duration.ofMillis(25);

  /** {@code order} is present only when the projection reached {@code requiredVersion}. */
  public record Result(
      Optional<ReadModels.OrderView> order,
      int requiredVersion,
      int projectedVersion,
      long waitedMs) {
    public boolean satisfied() {
      return order.isPresent();
    }
  }

  private final ReadModels readModels;

  public ReadYourWrites(ReadModels readModels) {
    this.readModels = readModels;
  }

  public Result await(UUID orderId, int requiredVersion, Duration wait)
      throws InterruptedException {
    long started = System.nanoTime();
    long deadline =
        started + Math.min(Math.max(wait.toMillis(), 0), MAX_WAIT.toMillis()) * 1_000_000;
    int projected = readModels.projectedVersion(orderId).orElse(0);
    while (projected < requiredVersion && System.nanoTime() < deadline) {
      Thread.sleep(POLL);
      projected = readModels.projectedVersion(orderId).orElse(0);
    }
    long waited = (System.nanoTime() - started) / 1_000_000;
    var order =
        projected >= requiredVersion
            ? readModels.order(orderId)
            : Optional.<ReadModels.OrderView>empty();
    return new Result(order, requiredVersion, projected, waited);
  }
}
