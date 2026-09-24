package io.zeroshift.order.infrastructure;

import io.zeroshift.contracts.Topics;
import io.zeroshift.order.application.OrderSaga;
import io.zeroshift.platform.Inbox;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * Consumes every participant's replies. Three threads share the nine partitions, so replies about
 * the same order on different topics can be handled at the same time: the saga's and the event
 * stream's optimistic locks make the loser retry against the winner's result.
 */
public class SagaReplies {
  public static final String CONSUMER = "order-saga";
  private final Inbox inbox;
  private final OrderSaga saga;

  public SagaReplies(Inbox inbox, OrderSaga saga) {
    this.inbox = inbox;
    this.saga = saga;
  }

  @KafkaListener(
      id = CONSUMER,
      topics = {Topics.PAYMENT_EVENTS, Topics.INVENTORY_EVENTS, Topics.SHIPPING_EVENTS},
      concurrency = "3")
  public void onReply(ConsumerRecord<String, String> record) {
    inbox.deliver(CONSUMER, record, saga::handle);
  }
}
