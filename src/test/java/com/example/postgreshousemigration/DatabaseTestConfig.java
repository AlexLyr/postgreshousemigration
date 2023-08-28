package com.example.postgreshousemigration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

import javax.sql.DataSource;

@TestConfiguration
public class DatabaseTestConfig {

    @Container
    public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13.2")
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test");

    @Container
    public static ClickHouseContainer clickhouse = new ClickHouseContainer("yandex/clickhouse-server:21.3");


    @Bean
    public DataSource clickhouseDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(clickhouse.getJdbcUrl());
        config.setUsername("default"); // ClickHouse default user
        config.setPassword(""); // No password by default
        return new HikariDataSource(config);
    }

    @Bean
    public DataSource postgresDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        return new HikariDataSource(config);
    }
}
