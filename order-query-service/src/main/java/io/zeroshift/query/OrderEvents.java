package io.zeroshift.query;

import io.zeroshift.contracts.Topics;
import io.zeroshift.platform.Inbox;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

public class OrderEvents {
  public static final String CONSUMER = "order-projection";
  public static final String TOPIC = Topics.ORDER_EVENTS;
  private final Inbox inbox;
  private final OrderProjection projection;

  public OrderEvents(Inbox inbox, OrderProjection projection) {
    this.inbox = inbox;
    this.projection = projection;
  }

  @KafkaListener(id = CONSUMER, topics = TOPIC, concurrency = "3")
  public void onEvent(ConsumerRecord<String, String> record) {
    inbox.deliver(CONSUMER, record, envelope -> projection.apply(envelope, record));
  }
}
