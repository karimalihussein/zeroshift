package io.zeroshift.order.infrastructure;

import io.zeroshift.contracts.Topics;
import io.zeroshift.order.application.OrderSaga;
import io.zeroshift.platform.Inbox;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * Consumes every participant's replies, spread over the order-service replicas by the consumer
 * group. Replies about one order may still be handled at the same time (after a rebalance, or when
 * partitions of different topics land on different replicas): the saga's and the event stream's
 * optimistic locks make the loser retry against the winner's result.
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
      // One consumer per replica: with two replicas the three partitions of each topic split 2 + 1
      // between them, and all move to the survivor when one dies. More threads than partitions
      // across the group would just sit idle.
      concurrency = "${order.saga-consumers:1}")
  public void onReply(ConsumerRecord<String, String> record) {
    inbox.deliver(CONSUMER, record, saga::handle);
  }
}
