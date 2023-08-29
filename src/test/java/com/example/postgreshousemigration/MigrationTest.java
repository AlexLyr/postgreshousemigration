package com.example.postgreshousemigration;

import org.junit.AfterClass;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MigrationTest {

    private static final Network network = Network.SHARED;

    private static PostgreSQLContainer<?> postgresContainer;
    private static ClickHouseContainer clickHouseContainer;

    static {
        postgresContainer = new PostgreSQLContainer<>("postgres:13.2")
                .withNetwork(network)
                .withNetworkAliases("mypostgres");

        clickHouseContainer = new ClickHouseContainer("yandex/clickhouse-server:21.3")
                .withNetwork(network)
                .withNetworkAliases("myclickhouse");

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
                    .driverClassName("ru.yandex.clickhouse.ClickHouseDriver")
                    .build();
        }
    }

    @Autowired
    private DataSource clickhouseDataSource;

    @Autowired
    private DataSource postgresDataSource;

    @BeforeEach
    public void initializeDatabase() throws SQLException {
        runScript(postgresDataSource, "postgresql-init.sql");
        runScript(clickhouseDataSource, "clickhouse-init.sql");
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
                     "INSERT INTO users(id, name, age, email) VALUES(?, ?, 23, ?)")) {
            stmt.setObject(1, userId);
            stmt.setString(2, name);
            stmt.setString(3, email);
            stmt.executeUpdate();
        }

        // Get connection details from PostgreSQL TestContainer
        String postgresUser = postgresContainer.getUsername();
        String postgresPassword = postgresContainer.getPassword();
        String postgresDb = "test"; // Assuming the DB name is "test"

        // Save data directly from PostgreSQL to ClickHouse
        try (Connection clickhouseConnection = clickhouseDataSource.getConnection();
             PreparedStatement stmt = clickhouseConnection.prepareStatement(
                     "INSERT INTO users SELECT * FROM postgresql(?, ?, ?, ?, ?) WHERE id = ?")) {
            stmt.setString(1, "mypostgres:5432");
            stmt.setString(2, postgresDb);
            stmt.setString(3, "users");  // Table name
            stmt.setString(4, postgresUser);
            stmt.setString(5, postgresPassword);
            stmt.setObject(6, userId);
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
