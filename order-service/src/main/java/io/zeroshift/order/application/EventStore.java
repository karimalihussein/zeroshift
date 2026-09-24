package io.zeroshift.order.application;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.order.domain.Order;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Append-only history per order stream, plus optional snapshots that shortcut replay. */
public interface EventStore {
  record Recorded(long position, long version, Envelope envelope, Instant recordedAt) {}

  record Snapshot(long version, Order state, Instant takenAt) {}

  List<Recorded> load(UUID stream, long afterVersion);

  /**
   * Appends after {@code expectedVersion}, or throws {@link ConcurrencyConflict} if the stream has
   * moved on.
   */
  void append(UUID stream, long expectedVersion, List<Envelope> events);

  Optional<Snapshot> snapshot(UUID stream);

  void saveSnapshot(Order state);

  /** Most recent streams first. */
  List<UUID> recentStreams(int limit);
}
