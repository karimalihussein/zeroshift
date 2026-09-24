package io.zeroshift.infrastructure;

import javax.sql.DataSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

public final class SchemaInitializer {
  public SchemaInitializer(DataSource target, DataSource source) {
    new ResourceDatabasePopulator(new ClassPathResource("db/target.sql")).execute(target);
    // SQL Server's IF/BEGIN block must be submitted as one statement.
    try {
      new JdbcTemplate(source)
          .execute(
              new ClassPathResource("db/source.sql")
                  .getContentAsString(java.nio.charset.StandardCharsets.UTF_8));
    } catch (java.io.IOException e) {
      throw new io.zeroshift.domain.MigrationException("Cannot read source schema", e);
    }
    for (var table : io.zeroshift.domain.Table.values()) {
      new JdbcTemplate(source)
          .execute(
              "CREATE OR ALTER TRIGGER dbo."
                  + table.sqlName()
                  + "_fence ON dbo."
                  + table.sqlName()
                  + " AFTER INSERT,UPDATE,DELETE AS BEGIN SET NOCOUNT ON;"
                  // Reverse sync is the one writer allowed past the fence after cutover.
                  + " IF CAST(SESSION_CONTEXT(N'"
                  + ReverseSyncWriter.SESSION_KEY
                  + "') AS NVARCHAR(32))=N'"
                  + ReverseSyncWriter.SESSION_VALUE
                  + "' RETURN; IF EXISTS(SELECT 1 FROM dbo.migration_gate WITH(HOLDLOCK) WHERE id=1 AND frozen=1) THROW 51000,'Source writes are frozen by ZeroShift',1; END");
    }
  }
}
