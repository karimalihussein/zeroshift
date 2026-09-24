package io.zeroshift.inventory.infrastructure;

import io.zeroshift.contracts.Topics;
import io.zeroshift.inventory.application.InventoryHandler;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.Inbox;
import io.zeroshift.platform.Outbox;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.*;

@Configuration(proxyBeanMethods = false)
@Import(InventoryWiring.StockController.class)
public class InventoryWiring {
  public static final String CONSUMER = "inventory-service";

  @Bean
  JdbcStock stock(JdbcTemplate jdbc) {
    return new JdbcStock(jdbc);
  }

  @Bean
  InventoryHandler inventoryHandler(JdbcStock stock, Outbox outbox, Faults faults) {
    return new InventoryHandler(stock, outbox, faults);
  }

  @Bean
  Commands inventoryCommands(Inbox inbox, InventoryHandler handler) {
    return new Commands(inbox, handler);
  }

  public static class Commands {
    private final Inbox inbox;
    private final InventoryHandler handler;

    Commands(Inbox inbox, InventoryHandler handler) {
      this.inbox = inbox;
      this.handler = handler;
    }

    @KafkaListener(id = CONSUMER, topics = Topics.INVENTORY_COMMANDS, concurrency = "3")
    public void onCommand(ConsumerRecord<String, String> record) {
      inbox.deliver(CONSUMER, record, handler::handle);
    }
  }

  @RestController
  public static class StockController {
    private final JdbcStock stock;

    public StockController(JdbcStock stock) {
      this.stock = stock;
    }

    @GetMapping("/stock")
    public List<Map<String, Object>> levels() {
      return stock.levels();
    }
  }
}
