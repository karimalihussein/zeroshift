package io.zeroshift.payment;

import io.zeroshift.platform.PlatformConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(PlatformConfiguration.class)
public class PaymentServiceApplication {
  public static void main(String[] args) {
    SpringApplication.run(PaymentServiceApplication.class, args);
  }
}
