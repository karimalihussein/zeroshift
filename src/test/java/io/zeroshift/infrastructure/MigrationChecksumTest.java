package io.zeroshift.infrastructure;

import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import org.junit.jupiter.api.Test;

/**
 * Applied migrations are immutable: Flyway refuses to start when a recorded checksum changes. Pin
 * every released script here so an edit fails this test instead of a developer's app startup. To
 * change the schema, add a new V&lt;n&gt;__*.sql and pin its checksum; never edit a pinned one.
 */
class MigrationChecksumTest {
  private static final Path MIGRATIONS = Path.of("src/main/resources/db/migration");

  /** Values as Flyway records them in flyway_schema_history.checksum. */
  private static final Map<String, Integer> RELEASED =
      Map.of(
          "V1__control_plane.sql", -1279102151,
          "V2__cdc_replay_control.sql", -1143865114,
          "V3__traffic_metrics.sql", 1949732719,
          "V4__completion_state.sql", 241417980,
          "V5__completed_rows.sql", -980669993,
          "V6__reverse_sync.sql", -80843725,
          "V7__cutover_cdc_pending.sql", 987926328);

  @Test
  void releasedMigrationsAreNeverEdited() throws IOException {
    for (var entry : RELEASED.entrySet()) {
      assertThat(flywayChecksum(MIGRATIONS.resolve(entry.getKey())))
          .as(
              "%s was applied with checksum %d; add a new migration instead",
              entry.getKey(), entry.getValue())
          .isEqualTo(entry.getValue());
    }
  }

  @Test
  void everyMigrationIsPinned() throws IOException {
    try (var files = Files.list(MIGRATIONS)) {
      assertThat(files.map(f -> f.getFileName().toString()).collect(Collectors.toSet()))
          .as("pin new migrations in RELEASED")
          .isEqualTo(RELEASED.keySet());
    }
  }

  /** Flyway's checksum: CRC32 over the script's lines, line terminators excluded. */
  private static int flywayChecksum(Path script) throws IOException {
    var crc = new CRC32();
    for (var line : Files.readAllLines(script, StandardCharsets.UTF_8)) {
      crc.update(line.getBytes(StandardCharsets.UTF_8));
    }
    return (int) crc.getValue();
  }
}
