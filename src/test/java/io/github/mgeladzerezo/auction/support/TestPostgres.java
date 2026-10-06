package io.github.mgeladzerezo.auction.support;

import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * One real PostgreSQL 16 for the whole test JVM. Started on first use and removed by
 * Testcontainers when the JVM exits. Every test that touches locking, transactions or SQL runs
 * against it; nothing in this project is tested against an in-memory substitute.
 */
public final class TestPostgres {

    private static final PostgreSQLContainer CONTAINER = new PostgreSQLContainer("postgres:16-alpine")
            .withDatabaseName("auction")
            .withUsername("auction")
            .withPassword("auction")
            // Several application contexts (and their pools) share this server during the build.
            .withCommand("postgres", "-c", "max_connections=300");

    static {
        CONTAINER.start();
    }

    private TestPostgres() {
    }

    public static String jdbcUrl() {
        return CONTAINER.getJdbcUrl();
    }

    public static String username() {
        return CONTAINER.getUsername();
    }

    public static String password() {
        return CONTAINER.getPassword();
    }
}
