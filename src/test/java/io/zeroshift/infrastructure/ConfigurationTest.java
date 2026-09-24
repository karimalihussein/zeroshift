package io.zeroshift.infrastructure;

import static org.assertj.core.api.Assertions.*;

import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;

class ConfigurationTest {
  @Test
  void rejectsInvalidConnectionAndUnboundedBatchSize() {
    try (var factory = Validation.buildDefaultValidatorFactory()) {
      var settings =
          new LabSettings(
              "jdbc:sqlserver://localhost;databaseName=production",
              "sa",
              "password",
              0,
              100,
              250,
              100);
      assertThat(factory.getValidator().validate(settings))
          .extracting(v -> v.getPropertyPath().toString())
          .containsExactlyInAnyOrder("sourceUrl", "batchSize");
    }
  }

  @Test
  void validatesSupportedSettings() {
    try (var factory = Validation.buildDefaultValidatorFactory()) {
      var settings =
          new LabSettings(
              "jdbc:sqlserver://localhost;databaseName=zeroshift_java;encrypt=true",
              "sa",
              "password",
              500,
              10000,
              250,
              100);
      assertThat(factory.getValidator().validate(settings)).isEmpty();
    }
  }
}
