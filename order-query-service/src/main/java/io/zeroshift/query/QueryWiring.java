package io.zeroshift.query;

import io.zeroshift.platform.Inbox;
import org.jooq.DSLContext;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration(proxyBeanMethods = false)
public class QueryWiring {
  @Bean
  ReadModels readModels(DSLContext db) {
    return new ReadModels(db);
  }

  @Bean
  OrderProjection orderProjection(ReadModels readModels) {
    return new OrderProjection(readModels);
  }

  @Bean
  OrderEvents orderEvents(Inbox inbox, OrderProjection projection) {
    return new OrderEvents(inbox, projection);
  }

  @Bean
  ProjectionRebuild projectionRebuild(
      KafkaListenerEndpointRegistry registry,
      KafkaAdmin kafkaAdmin,
      ReadModels readModels,
      Inbox inbox,
      TransactionTemplate transactions) {
    return new ProjectionRebuild(registry, kafkaAdmin, readModels, inbox, transactions);
  }
}
