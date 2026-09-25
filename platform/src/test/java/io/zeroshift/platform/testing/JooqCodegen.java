package io.zeroshift.platform.testing;

import java.nio.file.Path;
import java.util.Map;
import org.flywaydb.core.Flyway;
import org.jooq.codegen.GenerationTool;
import org.jooq.meta.jaxb.Configuration;
import org.jooq.meta.jaxb.Database;
import org.jooq.meta.jaxb.Generate;
import org.jooq.meta.jaxb.Generator;
import org.jooq.meta.jaxb.Jdbc;
import org.jooq.meta.jaxb.Target;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Flyway → PostgreSQL → jOOQ. Applies one module's migrations to a throwaway PostgreSQL (the same
 * image the lab runs) and generates typed classes for the tables they create. The migrations stay
 * the only definition of the schema; this just reads back what they produced. Run through the
 * {@code jooq-codegen} Maven profile.
 *
 * <p>Arguments: migrations directory, target package, output directory.
 */
public final class JooqCodegen {
  public static void main(String[] args) throws Exception {
    var migrations = Path.of(args[0]).toAbsolutePath();
    try (var postgres = new PostgreSQLContainer<>(CommerceStack.POSTGRES_IMAGE)) {
      postgres.start();
      Flyway.configure()
          .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
          .locations("filesystem:" + migrations)
          // The platform's replication-slot migration is Java and not on this path; nothing else
          // reads the placeholder, but Flyway wants every placeholder it may meet defined.
          .placeholders(Map.of("outboxSlot", "false"))
          .load()
          .migrate();
      GenerationTool.generate(
          new Configuration()
              .withJdbc(
                  new Jdbc()
                      .withDriver("org.postgresql.Driver")
                      .withUrl(postgres.getJdbcUrl())
                      .withUser(postgres.getUsername())
                      .withPassword(postgres.getPassword()))
              .withGenerator(
                  new Generator()
                      .withDatabase(
                          new Database()
                              .withName("org.jooq.meta.postgres.PostgresDatabase")
                              .withInputSchema("public")
                              .withExcludes("flyway_.*"))
                      .withGenerate(
                          new Generate()
                              .withRecords(true)
                              .withPojos(false)
                              .withDaos(false)
                              .withJavaTimeTypes(true)
                              .withGeneratedAnnotation(false))
                      .withTarget(
                          new Target()
                              .withPackageName(args[1])
                              .withDirectory(args[2])
                              .withClean(true))));
    }
  }

  private JooqCodegen() {}
}
