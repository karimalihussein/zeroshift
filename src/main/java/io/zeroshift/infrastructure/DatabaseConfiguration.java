package io.zeroshift.infrastructure;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class DatabaseConfiguration {
  @Bean
  @Primary
  @org.springframework.boot.context.properties.ConfigurationProperties("spring.datasource.hikari")
  DataSource dataSource(DataSourceProperties properties) {
    return properties.initializeDataSourceBuilder().build();
  }

  @Bean(name = "sourceDataSource", destroyMethod = "close")
  HikariDataSource sourceDataSource(LabSettings settings) {
    // Bootstrap connects to master first; the application then uses only its own lab database.
    var master =
        new org.springframework.jdbc.core.JdbcTemplate(
            new org.springframework.jdbc.datasource.DriverManagerDataSource(
                settings.sourceUrl().replace("databaseName=zeroshift_java", "databaseName=master"),
                settings.sourceUser(),
                settings.sourcePassword()));
    master.execute("IF DB_ID('zeroshift_java') IS NULL CREATE DATABASE zeroshift_java");
    master.execute("ALTER DATABASE zeroshift_java SET ALLOW_SNAPSHOT_ISOLATION ON");
    master.execute(
        "IF NOT EXISTS(SELECT 1 FROM sys.change_tracking_databases WHERE database_id=DB_ID('zeroshift_java')) ALTER DATABASE zeroshift_java SET CHANGE_TRACKING=ON (CHANGE_RETENTION=7 DAYS,AUTO_CLEANUP=ON)");
    var config = new HikariConfig();
    config.setJdbcUrl(settings.sourceUrl());
    config.setUsername(settings.sourceUser());
    config.setPassword(settings.sourcePassword());
    config.setMaximumPoolSize(6);
    config.setConnectionTimeout(10000);
    config.setPoolName("sql-server");
    return new HikariDataSource(config);
  }

  @Bean
  SchemaInitializer schemaInitializer(
      DataSource dataSource, @Qualifier("sourceDataSource") DataSource source) {
    return new SchemaInitializer(dataSource, source);
  }
}
