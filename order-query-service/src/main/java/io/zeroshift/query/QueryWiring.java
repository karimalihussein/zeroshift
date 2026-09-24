package io.zeroshift.query;

import io.zeroshift.platform.Inbox;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration(proxyBeanMethods = false)
public class QueryWiring {
  @Bean
  OrderProjection orderProjection(JdbcTemplate jdbc) {
    return new OrderProjection(jdbc);
  }

  @Bean
  OrderEvents orderEvents(Inbox inbox, OrderProjection projection) {
    return new OrderEvents(inbox, projection);
  }

  @Bean
  ProjectionRebuild projectionRebuild(
      KafkaListenerEndpointRegistry registry,
      KafkaAdmin kafkaAdmin,
      JdbcTemplate jdbc,
      TransactionTemplate transactions) {
    return new ProjectionRebuild(registry, kafkaAdmin, jdbc, transactions);
  }
}
