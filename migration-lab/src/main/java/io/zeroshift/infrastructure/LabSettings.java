package io.zeroshift.infrastructure;

import jakarta.validation.constraints.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("lab")
public record LabSettings(
    @NotBlank
        @Pattern(
            regexp = "jdbc:sqlserver://.+;databaseName=zeroshift_java(;.*)?",
            message = "must connect to the dedicated zeroshift_java database")
        String sourceUrl,
    @NotBlank String sourceUser,
    @NotBlank String sourcePassword,
    @Min(1) @Max(10000) int batchSize,
    @Min(1) @Max(io.zeroshift.application.DemoDataService.MAX_ROWS) int seedRows,
    @Min(10) long tickMs,
    @Min(10) long trafficMs) {}
