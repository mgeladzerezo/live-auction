package io.github.mgeladzerezo.auction.support;

import io.github.mgeladzerezo.auction.auction.Auction;
import io.github.mgeladzerezo.auction.auction.AuctionLifecycle;
import io.github.mgeladzerezo.auction.auction.AuctionRepository;
import io.github.mgeladzerezo.auction.auction.AuctionStatus;
import io.github.mgeladzerezo.auction.auction.NewAuction;
import io.github.mgeladzerezo.auction.auth.User;
import io.github.mgeladzerezo.auction.bid.BidCheckpoint;
import io.github.mgeladzerezo.auction.bid.BidCommand;
import io.github.mgeladzerezo.auction.bid.BidLedger;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Base for tests that need the real application on a random port and a real PostgreSQL.
 *
 * <p>The lifecycle scheduler is switched off so that a test decides exactly when auctions open
 * and close (by calling {@link AuctionLifecycle} itself). All subclasses share one cached
 * application context.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "auction.lifecycle.enabled=false",
        "auction.demo.enabled=false",
        "auction.auth.pbkdf2-iterations=1000",
        "auction.instance-id=test",
        "spring.datasource.hikari.minimum-idle=2"
})
public abstract class AbstractIntegrationTest {

    private static final AtomicLong USER_COUNTER = new AtomicLong();

    @Autowired
    protected JdbcClient jdbc;
    @Autowired
    protected AuctionRepository auctions;
    @Autowired
    protected AuctionLifecycle lifecycle;
    @Autowired
    protected BidLedger ledger;

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", TestPostgres::jdbcUrl);
        registry.add("spring.datasource.username", TestPostgres::username);
        registry.add("spring.datasource.password", TestPostgres::password);
    }

    @AfterEach
    void removeCheckpoint() {
        ledger.installCheckpoint(BidCheckpoint.NONE);
    }

    /** Inserts a user directly (no password hashing) and returns it. */
    protected User newUser(String prefix) {
        String username = prefix + "-" + USER_COUNTER.incrementAndGet() + "-" + Long.toString(System.nanoTime(), 36);
        long id = jdbc.sql("INSERT INTO users (username, password_hash) VALUES (:username, '!') RETURNING id")
                .param("username", username)
                .query(Long.class)
                .single();
        return new User(id, username);
    }

    /** A plain auction: start 1000, increment 100, no reserve, no anti-sniping, one hour long. */
    protected static NewAuction.Builder auction() {
        return new NewAuction.Builder();
    }

    /** Creates the auction and opens it through the real lifecycle. */
    protected Auction open(NewAuction.Builder spec) {
        long id = auctions.insert(spec.startsIn(Duration.ZERO).build());
        lifecycle.openDue();
        Auction auction = auctions.find(id).orElseThrow();
        if (auction.status() != AuctionStatus.OPEN) {
            throw new IllegalStateException("Auction " + id + " did not open: " + auction.status());
        }
        return auction;
    }

    protected Auction reload(long auctionId) {
        return auctions.find(auctionId).orElseThrow();
    }

    protected static BidCommand bid(Auction auction, User bidder, long amount) {
        return new BidCommand(auction.id(), bidder.id(), bidder.username(), amount, UUID.randomUUID().toString());
    }

    protected Instant dbNow() {
        return jdbc.sql("SELECT clock_timestamp()").query(OffsetDateTime.class).single().toInstant();
    }

    /** Blocks until the database clock has passed the given instant. */
    protected void awaitDbTime(Instant instant) throws InterruptedException {
        while (!dbNow().isAfter(instant)) {
            Thread.sleep(10);
        }
    }

    protected long count(String table, long auctionId) {
        return jdbc.sql("SELECT count(*) FROM " + table + " WHERE auction_id = :id")
                .param("id", auctionId)
                .query(Long.class)
                .single();
    }
}
