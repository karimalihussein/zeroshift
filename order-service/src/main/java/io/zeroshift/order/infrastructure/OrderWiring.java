package io.zeroshift.order.infrastructure;

import io.zeroshift.order.application.*;
import io.zeroshift.platform.Inbox;
import io.zeroshift.platform.Outbox;
import java.time.Clock;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration(proxyBeanMethods = false)
public class OrderWiring {
  @Bean
  Clock clock() {
    return Clock.systemUTC();
  }

  @Bean
  EventStore eventStore(JdbcTemplate jdbc) {
    return new PostgresEventStore(jdbc);
  }

  @Bean
  SagaStore sagaStore(JdbcTemplate jdbc) {
    return new PostgresSagaStore(jdbc);
  }

  @Bean
  Catalog catalog(JdbcTemplate jdbc) {
    return new PostgresCatalog(jdbc);
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
      OrderRepository orders,
      SagaStore sagas,
      Outbox outbox,
      OrderSaga saga,
      TransactionTemplate transactions,
      Clock clock) {
    return new PlaceOrder(catalog, orders, sagas, outbox, saga, transactions, clock);
  }

  @Bean
  SagaReplies sagaReplies(Inbox inbox, OrderSaga saga) {
    return new SagaReplies(inbox, saga);
  }

  @Bean
  SagaTimeouts sagaTimeouts(
      SagaStore sagas, OrderSaga saga, TransactionTemplate transactions, Clock clock) {
    return new SagaTimeouts(sagas, saga, transactions, clock);
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
