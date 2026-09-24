package io.zeroshift;

import io.zeroshift.infrastructure.LabSettings;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(LabSettings.class)
public class ZeroShiftApplication {
  public static void main(String[] args) {
    SpringApplication.run(ZeroShiftApplication.class, args);
  }
}
