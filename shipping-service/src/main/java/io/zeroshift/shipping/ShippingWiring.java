package io.zeroshift.shipping;

import io.zeroshift.contracts.Topics;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.Inbox;
import io.zeroshift.platform.Outbox;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;

@Configuration(proxyBeanMethods = false)
public class ShippingWiring {
  public static final String CONSUMER = "shipping-service";

  @Bean
  ShippingHandler shippingHandler(JdbcTemplate jdbc, Outbox outbox, Faults faults) {
    return new ShippingHandler(jdbc, outbox, faults);
  }

  @Bean
  Commands shippingCommands(Inbox inbox, ShippingHandler handler) {
    return new Commands(inbox, handler);
  }

  public static class Commands {
    private final Inbox inbox;
    private final ShippingHandler handler;

    Commands(Inbox inbox, ShippingHandler handler) {
      this.inbox = inbox;
      this.handler = handler;
    }

    @KafkaListener(id = CONSUMER, topics = Topics.SHIPPING_COMMANDS, concurrency = "3")
    public void onCommand(ConsumerRecord<String, String> record) {
      inbox.deliver(CONSUMER, record, handler::handle);
    }
  }
}
