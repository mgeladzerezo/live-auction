package io.github.mgeladzerezo.auction.config;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * An estimate of the database clock that can be read without a round trip.
 *
 * <p><b>Never used to decide anything.</b> Whether a bid is in time and whether an auction is
 * due to close are decided by {@code clock_timestamp()} / {@code statement_timestamp()} inside
 * the SQL statement that makes the change. This estimate only feeds things where a few
 * milliseconds of error are harmless: the {@code serverTime} clients use to align their
 * countdown, and the broadcast-lag metric.
 *
 * <p>The offset between the JVM clock and the database clock is re-measured periodically using
 * the midpoint of a query's round trip, the same idea as NTP's.
 */
@Component
public class DbClock {

    private static final Logger log = LoggerFactory.getLogger(DbClock.class);

    private final JdbcClient jdbc;
    private volatile Duration offset = Duration.ZERO;

    public DbClock(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Current database time, estimated from the local clock and the last measured offset. */
    public Instant now() {
        return Instant.now().plus(offset);
    }

    /** Exact database time, at the cost of a query. */
    public Instant read() {
        return jdbc.sql("SELECT clock_timestamp()").query(OffsetDateTime.class).single().toInstant();
    }

    @Scheduled(initialDelay = 0, fixedDelayString = "PT30S")
    void measureOffset() {
        try {
            Instant before = Instant.now();
            Instant database = read();
            Instant after = Instant.now();
            Instant localMidpoint = before.plus(Duration.between(before, after).dividedBy(2));
            offset = Duration.between(localMidpoint, database);
        } catch (RuntimeException e) {
            log.warn("Could not measure database clock offset, keeping {}: {}", offset, e.toString());
        }
    }
}
