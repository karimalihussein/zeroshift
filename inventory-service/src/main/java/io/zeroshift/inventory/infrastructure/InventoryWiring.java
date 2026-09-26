package io.zeroshift.inventory.infrastructure;

import io.zeroshift.contracts.Topics;
import io.zeroshift.inventory.application.InventoryHandler;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.Inbox;
import io.zeroshift.platform.Outbox;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.*;
import org.springframework.kafka.annotation.KafkaListener;

@Configuration(proxyBeanMethods = false)
@Import({ProductController.class, StockController.class})
public class InventoryWiring {
  public static final String CONSUMER = "inventory-service";

  @Bean
  PostgresStock stock(DSLContext db) {
    return new PostgresStock(db);
  }

  @Bean
  ProductCatalog productCatalog(DSLContext db, @Value("${commerce.currency:USD}") String currency) {
    return new ProductCatalog(db, currency);
  }

  /** Off in tests, on in Compose: tests insert the products they need. */
  @Bean
  ApplicationRunner demoCatalog(
      DSLContext db,
      @Value("${commerce.demo-data:false}") boolean enabled,
      @Value("${commerce.demo-seed:#{null}}") Long seed) {
    return args -> {
      if (enabled) new DemoCatalog(db, seed).seedIfEmpty();
    };
  }

  @Bean
  InventoryHandler inventoryHandler(PostgresStock stock, Outbox outbox, Faults faults) {
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
}
