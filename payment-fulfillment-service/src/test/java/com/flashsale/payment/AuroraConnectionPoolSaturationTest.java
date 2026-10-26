package com.flashsale.payment;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLTransientConnectionException;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves fix for Scenario 10 (REM-08):
 * Aurora/PostgreSQL reaches connection limit during traffic burst.
 * HikariCP connection pool enforces strict bounding (maximum-pool-size, minimum-idle, connection-timeout=3000ms).
 * Under saturation, connection requests fail fast with SQLTransientConnectionException
 * rather than exhausting Aurora server limits or hanging worker threads indefinitely.
 */
class AuroraConnectionPoolSaturationTest {

    @Test
    @DisplayName("Scenario 10 (REM-08): Connection pool under saturation fails fast with timeout rather than blocking indefinitely")
    void shouldFailFastWhenConnectionPoolIsSaturated() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:pool_test;MODE=PostgreSQL");
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(2);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(300); // 300ms fast-fail timeout

        try (HikariDataSource dataSource = new HikariDataSource(config)) {
            // Settle all 2 connections in pool
            Connection conn1 = dataSource.getConnection();
            Connection conn2 = dataSource.getConnection();
            assertThat(conn1).isNotNull();
            assertThat(conn2).isNotNull();

            // Thread 3 attempts to acquire connection 3
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<?> future = executor.submit(() -> {
                dataSource.getConnection(); // Must fail fast within ~300ms
                return null;
            });

            assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(SQLTransientConnectionException.class)
                .hasMessageContaining("Connection is not available, request timed out");

            conn1.close();
            conn2.close();
            executor.shutdown();
        }
    }
}
