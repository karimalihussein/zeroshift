package io.zeroshift.payment.infrastructure;

import io.zeroshift.contracts.Topics;
import io.zeroshift.payment.application.PaymentHandler;
import io.zeroshift.platform.Inbox;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

public class PaymentCommands {
  public static final String CONSUMER = "payment-service";
  private final Inbox inbox;
  private final PaymentHandler handler;

  public PaymentCommands(Inbox inbox, PaymentHandler handler) {
    this.inbox = inbox;
    this.handler = handler;
  }

  @KafkaListener(id = CONSUMER, topics = Topics.PAYMENT_COMMANDS, concurrency = "3")
  public void onCommand(ConsumerRecord<String, String> record) {
    inbox.deliver(CONSUMER, record, handler::handle);
  }
}
