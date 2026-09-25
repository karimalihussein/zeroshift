package io.zeroshift.order.application;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.OrderEvent;
import io.zeroshift.order.domain.Order;
import io.zeroshift.platform.Outbox;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Loads an order by folding its history, and records new events: each one is appended to the event
 * store (the source of truth) and to the outbox (its publication) in the caller's transaction.
 */
public final class OrderRepository {
  /** An order and how it was rebuilt: from which snapshot, replaying which events. */
  public record Loaded(
      Order order, EventStore.Snapshot snapshot, List<EventStore.Recorded> replayed) {}

  /** The order after the new events, and the envelopes they were recorded and published in. */
  public record Appended(Order order, List<Envelope> events) {}

  private final EventStore events;
  private final Outbox outbox;
  private final int snapshotEvery;

  public OrderRepository(EventStore events, Outbox outbox, int snapshotEvery) {
    this.events = events;
    this.outbox = outbox;
    this.snapshotEvery = snapshotEvery;
  }

  public Loaded load(UUID id, boolean useSnapshot) {
    var snapshot = useSnapshot ? events.snapshot(id).orElse(null) : null;
    var order = snapshot == null ? Order.empty(id) : snapshot.state();
    var replayed = events.load(id, order.version());
    for (var recorded : replayed) order = order.apply((OrderEvent) recorded.envelope().payload());
    return new Loaded(order, snapshot, replayed);
  }

  /** One step of a fold: the aggregate right after one more event. */
  public record Step(long version, String type, Order stateAfter) {}

  /** Every intermediate state: the aggregate rebuilt one event at a time, no snapshot. */
  public List<Step> fold(UUID id) {
    var steps = new ArrayList<Step>();
    var order = Order.empty(id);
    for (var recorded : events.load(id, 0)) {
      order = order.apply((OrderEvent) recorded.envelope().payload());
      steps.add(new Step(recorded.version(), recorded.envelope().type(), order));
    }
    if (steps.isEmpty()) throw new OrderNotFound(id);
    return steps;
  }

  public Order load(UUID id) {
    return load(id, true).order();
  }

  /**
   * Records {@code newEvents} on top of {@code current}. {@code cause} is the message that led to
   * them (null for a new request, which starts the conversation {@code correlationId}).
   */
  public Appended record(
      Order current, Envelope cause, UUID correlationId, OrderEvent... newEvents) {
    var envelopes = new ArrayList<Envelope>();
    var next = current;
    for (var event : newEvents) {
      envelopes.add(cause == null ? Envelope.of(event, correlationId, null) : cause.reply(event));
      next = next.apply(event);
    }
    events.append(current.id(), current.version(), envelopes);
    envelopes.forEach(outbox::append);
    if (next.version() / snapshotEvery > current.version() / snapshotEvery)
      events.saveSnapshot(next);
    return new Appended(next, List.copyOf(envelopes));
  }
}
