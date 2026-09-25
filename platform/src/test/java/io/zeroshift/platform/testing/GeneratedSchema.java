package io.zeroshift.platform.testing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Schema;
import org.jooq.Table;

/**
 * The generated jOOQ classes are committed, so they can fall behind the migrations. This compares
 * them with the schema Flyway actually built: every table and column, both ways. On failure, run
 * {@code mvn -Pjooq-codegen -DskipTests process-test-classes} and commit the result.
 */
public final class GeneratedSchema {
  public static void assertMatchesDatabase(DSLContext db, Schema... generated) {
    var expected = new TreeMap<String, String>();
    for (var schema : generated)
      schema.getTables().forEach(t -> expected.put(t.getName(), columns(t)));
    var actual =
        db.meta().getSchemas("public").stream()
            .flatMap(s -> s.getTables().stream())
            .filter(t -> !t.getName().startsWith("flyway_"))
            .collect(
                Collectors.toMap(
                    Table::getName, GeneratedSchema::columns, (a, b) -> a, TreeMap::new));
    assertThat((Map<String, String>) actual)
        .as("tables and columns: generated jOOQ classes vs the migrated database")
        .isEqualTo(expected);
  }

  private static String columns(Table<?> table) {
    return Arrays.stream(table.fields())
        .map(Field::getName)
        .sorted()
        .collect(Collectors.joining(","));
  }

  private GeneratedSchema() {}
}
