package io.zeroshift.query;

import io.zeroshift.platform.PlatformConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(PlatformConfiguration.class)
public class OrderQueryApplication {
  public static void main(String[] args) {
    SpringApplication.run(OrderQueryApplication.class, args);
  }
}
