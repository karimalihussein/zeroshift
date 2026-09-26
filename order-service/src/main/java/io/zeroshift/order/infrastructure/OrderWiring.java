package io.zeroshift.order.infrastructure;

import io.micrometer.core.instrument.MeterRegistry;
import io.zeroshift.order.application.*;
import io.zeroshift.order.domain.Pricing;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.Inbox;
import io.zeroshift.platform.Outbox;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
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
  Catalog catalog(@Value("${commerce.catalog-url:http://localhost:18084}") URI catalogUrl) {
    return new HttpCatalog(catalogUrl);
  }

  @Bean
  Customers customers(DSLContext db) {
    return new PostgresCustomers(db);
  }

  @Bean
  Vouchers vouchers(DSLContext db) {
    return new PostgresVouchers(db);
  }

  @Bean
  OrderTables orderTables(DSLContext db) {
    return new PostgresOrderTables(db);
  }

  /** The lab sells in one currency at one tax rate; both are copied onto every order. */
  @Bean
  Pricing pricing(
      @Value("${commerce.currency:USD}") String currency,
      @Value("${commerce.tax-rate:0.0800}") BigDecimal taxRate) {
    return new Pricing(currency, taxRate);
  }

  @Bean
  @ConditionalOnBooleanProperty("commerce.demo-data")
  DemoData demoData(
      DSLContext db,
      TransactionTemplate transactions,
      @Value("${commerce.demo-seed:#{null}}") Long seed) {
    return new DemoData(db, transactions, seed);
  }

  @Bean
  OrderRepository orderRepository(
      EventStore events,
      Outbox outbox,
      OrderTables tables,
      @Value("${order.snapshot-every:3}") int snapshotEvery) {
    return new OrderRepository(events, outbox, tables, snapshotEvery);
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
      Customers customers,
      Vouchers vouchers,
      OrderTables tables,
      Pricing pricing,
      IdempotencyKeys keys,
      OrderRepository orders,
      SagaStore sagas,
      Outbox outbox,
      OrderSaga saga,
      TransactionTemplate transactions,
      Clock clock) {
    return new PlaceOrder(
        catalog,
        customers,
        vouchers,
        tables,
        pricing,
        keys,
        orders,
        sagas,
        outbox,
        saga,
        transactions,
        clock);
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
      PlaceOrder placeOrder,
      EventStore events,
      OrderTables tables,
      KafkaTemplate<String, String> kafka,
      TransactionTemplate transactions) {
    return new DualWriteDemo(placeOrder, events, tables, kafka, transactions);
  }
}
