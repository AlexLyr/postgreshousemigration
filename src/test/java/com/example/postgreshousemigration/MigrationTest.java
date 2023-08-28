package com.example.postgreshousemigration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest
@Import(MigrationTest.DatabaseTestConfig.class)
public class MigrationTest {


    private static PostgreSQLContainer<?> postgresContainer;
    private static ClickHouseContainer clickHouseContainer;

    static {
        postgresContainer = new PostgreSQLContainer<>("postgres:13.2");
        clickHouseContainer = new ClickHouseContainer("yandex/clickhouse-server:21.3");

        postgresContainer.start();
        clickHouseContainer.start();
    }

    @AfterClass
    public static void cleanup() {
        postgresContainer.stop();
        clickHouseContainer.stop();
    }

    @TestConfiguration
    public static class DatabaseTestConfig {

        @Bean
        public DataSource postgresDataSource() {
            return DataSourceBuilder.create()
                    .url(postgresContainer.getJdbcUrl())
                    .username(postgresContainer.getUsername())
                    .password(postgresContainer.getPassword())
                    .driverClassName(postgresContainer.getDriverClassName())
                    .build();
        }

        @Bean
        public DataSource clickhouseDataSource() {
            return DataSourceBuilder.create()
                    .url(clickHouseContainer.getJdbcUrl())
                    .username(clickHouseContainer.getUsername())
                    .password(clickHouseContainer.getPassword())
                    .driverClassName(clickHouseContainer.getDriverClassName())
                    .build();
        }
    }

    @Autowired
    private DataSource clickhouseDataSource;

    @Autowired
    private DataSource postgresDataSource;

    @BeforeEach
    public void initializeDatabase() throws SQLException {
        runScript(clickhouseDataSource, "clickhouse-init.sql");
        runScript(postgresDataSource, "postgresql-init.sql");
    }

    private void runScript(DataSource dataSource, String scriptLocation) throws SQLException {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new ClassPathResource(scriptLocation));
        DatabasePopulatorUtils.execute(populator, dataSource);
    }

    @Test
    public void migrateAndVerifyUser() throws SQLException {
        // Create and save user to PostgreSQL
        UUID userId = UUID.randomUUID();
        String name = "John";
        String email = "john@example.com";

        try (Connection postgresConnection = postgresDataSource.getConnection();
             PreparedStatement stmt = postgresConnection.prepareStatement(
                     "INSERT INTO users(id, name, email) VALUES(?, ?, ?)")) {
            stmt.setObject(1, userId);
            stmt.setString(2, name);
            stmt.setString(3, email);
            stmt.executeUpdate();
        }

        // Fetch from PostgreSQL
        String fetchedName = null;
        String fetchedEmail = null;

        try (Connection postgresConnection = postgresDataSource.getConnection();
             PreparedStatement stmt = postgresConnection.prepareStatement(
                     "SELECT name, email FROM users WHERE id = ?")) {
            stmt.setObject(1, userId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                fetchedName = rs.getString("name");
                fetchedEmail = rs.getString("email");
            }
        }

        // Save to ClickHouse
        try (Connection clickhouseConnection = clickhouseDataSource.getConnection();
             PreparedStatement stmt = clickhouseConnection.prepareStatement(
                     "INSERT INTO users(id, name, email) VALUES(?, ?, ?)")) {
            stmt.setObject(1, userId);
            stmt.setString(2, fetchedName);
            stmt.setString(3, fetchedEmail);
            stmt.executeUpdate();
        }

        // Fetch from ClickHouse and Verify
        try (Connection clickhouseConnection = clickhouseDataSource.getConnection();
             PreparedStatement stmt = clickhouseConnection.prepareStatement(
                     "SELECT name, email FROM users WHERE id = ?")) {
            stmt.setObject(1, userId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                assertEquals(name, rs.getString("name"));
                assertEquals(email, rs.getString("email"));
            }
        }
    }

}