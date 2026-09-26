package io.zeroshift.order.infrastructure;

import io.micrometer.core.instrument.MeterRegistry;
import io.zeroshift.order.application.*;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.Inbox;
import io.zeroshift.platform.Outbox;
import java.time.Clock;
import java.time.Duration;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration(proxyBeanMethods = false)
public class OrderWiring {
  @Bean
  Clock clock() {
    return Clock.systemUTC();
  }

  @Bean
  SagaPressure sagaPressure(DSLContext db, MeterRegistry meters) {
    return new SagaPressure(db, meters);
  }

  @Bean
  ShippedSales shippedSales(DSLContext db) {
    return new ShippedSales(db);
  }

  @Bean
  EventStore eventStore(DSLContext db) {
    return new PostgresEventStore(db);
  }

  @Bean
  SagaStore sagaStore(DSLContext db) {
    return new PostgresSagaStore(db);
  }

  @Bean
  Catalog catalog(DSLContext db) {
    return new PostgresCatalog(db);
  }

  @Bean
  OrderRepository orderRepository(
      EventStore events, Outbox outbox, @Value("${order.snapshot-every:3}") int snapshotEvery) {
    return new OrderRepository(events, outbox, snapshotEvery);
  }

  @Bean
  OrderSaga orderSaga(
      OrderRepository orders,
      SagaStore sagas,
      Outbox outbox,
      @Value("${order.step-timeout:30s}") Duration stepTimeout,
      Clock clock) {
    return new OrderSaga(orders, sagas, outbox, stepTimeout, clock);
  }

  @Bean
  PlaceOrder placeOrder(
      Catalog catalog,
      IdempotencyKeys keys,
      OrderRepository orders,
      SagaStore sagas,
      Outbox outbox,
      OrderSaga saga,
      TransactionTemplate transactions,
      Clock clock) {
    return new PlaceOrder(catalog, keys, orders, sagas, outbox, saga, transactions, clock);
  }

  @Bean
  IdempotencyKeys idempotencyKeys(DSLContext db) {
    return new PostgresIdempotencyKeys(db);
  }

  @Bean
  OperatorRefund operatorRefund(SagaStore sagas, Outbox outbox, TransactionTemplate transactions) {
    return new OperatorRefund(sagas, outbox, transactions);
  }

  @Bean
  SagaReplies sagaReplies(Inbox inbox, OrderSaga saga) {
    return new SagaReplies(inbox, saga);
  }

  @Bean
  PostgresLease lease(
      DSLContext db, @Value("${zeroshift.instance:${HOSTNAME:local}}") String instance) {
    return new PostgresLease(db, instance);
  }

  @Bean
  SagaTimeouts sagaTimeouts(
      SagaStore sagas,
      OrderSaga saga,
      TransactionTemplate transactions,
      Clock clock,
      PostgresLease lease,
      Faults faults,
      MeterRegistry meters) {
    return new SagaTimeouts(sagas, saga, transactions, clock, lease, faults, meters);
  }

  @Bean
  DualWriteDemo dualWriteDemo(
      PlaceOrder pricing,
      EventStore events,
      KafkaTemplate<String, String> kafka,
      TransactionTemplate transactions) {
    return new DualWriteDemo(pricing, events, kafka, transactions);
  }
}
