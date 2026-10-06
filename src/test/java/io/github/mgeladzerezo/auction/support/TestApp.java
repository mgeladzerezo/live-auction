package io.github.mgeladzerezo.auction.support;

import io.github.mgeladzerezo.auction.LiveAuctionApplication;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Starts the real application on a random port against the shared test PostgreSQL, outside the
 * Spring test context cache. Used where a test needs several instances at once, or needs to
 * choose the locking strategy per run.
 */
public final class TestApp implements AutoCloseable {

    private final ConfigurableApplicationContext context;

    private TestApp(ConfigurableApplicationContext context) {
        this.context = context;
    }

    /** Starts an instance; {@code properties} are {@code key=value} overrides. */
    public static TestApp start(String instanceId, String... properties) {
        List<String> arguments = new ArrayList<>(List.of(
                "--server.port=0",
                "--spring.datasource.url=" + TestPostgres.jdbcUrl(),
                "--spring.datasource.username=" + TestPostgres.username(),
                "--spring.datasource.password=" + TestPostgres.password(),
                "--spring.datasource.hikari.minimum-idle=2",
                "--spring.main.banner-mode=off",
                "--spring.jmx.enabled=false",
                "--auction.instance-id=" + instanceId,
                "--auction.demo.enabled=false",
                "--auction.auth.pbkdf2-iterations=1000"));
        for (String property : properties) {
            arguments.add("--" + property);
        }
        return new TestApp(new SpringApplicationBuilder(LiveAuctionApplication.class)
                .run(arguments.toArray(String[]::new)));
    }

    public int port() {
        return ((ServletWebServerApplicationContext) context).getWebServer().getPort();
    }

    public <T> T bean(Class<T> type) {
        return context.getBean(type);
    }

    @Override
    public void close() {
        context.close();
    }
}
