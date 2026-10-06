package io.github.mgeladzerezo.auction.demo;

import io.github.mgeladzerezo.auction.auction.AuctionService;
import io.github.mgeladzerezo.auction.auction.NewAuction;
import io.github.mgeladzerezo.auction.config.AuctionProperties;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Keeps the demo alive: each seeded lot has a stable {@code demo_key}, and whenever the newest
 * auction with that key has finished for longer than {@code auction.demo.reopen-delay} (or none
 * exists) a fresh copy is created and opens on the next scheduler pass.
 *
 * <p>Every instance runs this. A transaction-scoped advisory lock makes the check-then-create
 * sequence single-file across instances, so two instances never both reopen the same lot.
 */
@Component
@ConditionalOnProperty(name = "auction.demo.enabled", havingValue = "true")
class DemoSeeder {

    private static final Logger log = LoggerFactory.getLogger(DemoSeeder.class);
    private static final long ADVISORY_KEY = 4_155_437_001L;
    private static final int ANTI_SNIPE_SECONDS = 15;
    private static final int MAX_EXTENSIONS = 6;

    /** One recurring lot. Prices are minor units (cents). */
    record Lot(String key, String title, String description, long startPrice, long increment, Long reserve,
               Duration duration) {
    }

    static final List<Lot> LOTS = List.of(
            new Lot("leica", "Leica M3 rangefinder, 1962", "Serviced, with a 50mm f/2 Summicron.",
                    50_000, 2_500, 90_000L, Duration.ofSeconds(150)),
            new Lot("dune", "First edition of Dune", "Chilton, 1965. Dust jacket in good condition.",
                    12_000, 500, null, Duration.ofSeconds(100)),
            new Lot("armchair", "Mid-century lounge armchair", "Teak frame, original wool upholstery.",
                    30_000, 1_000, 45_000L, Duration.ofSeconds(120)),
            new Lot("keyboard", "Hand-built mechanical keyboard", "75% layout, brass plate, lubed linear switches.",
                    8_000, 250, null, Duration.ofSeconds(80)));

    private final AuctionService auctions;
    private final DemoUsers users;
    private final JdbcClient jdbc;
    private final TransactionTemplate transaction;
    private final Duration reopenDelay;

    DemoSeeder(AuctionService auctions, DemoUsers users, JdbcClient jdbc,
               PlatformTransactionManager transactionManager, AuctionProperties properties) {
        this.auctions = auctions;
        this.users = users;
        this.jdbc = jdbc;
        this.transaction = new TransactionTemplate(transactionManager);
        this.reopenDelay = properties.demo().reopenDelay();
    }

    @EventListener(ApplicationReadyEvent.class)
    void seedAtStartup() {
        tick();
    }

    @Scheduled(fixedDelay = 5_000, initialDelay = 5_000)
    void tick() {
        try {
            transaction.executeWithoutResult(status -> {
                Boolean locked = jdbc.sql("SELECT pg_try_advisory_xact_lock(:key)").param("key", ADVISORY_KEY)
                        .query(Boolean.class).single();
                if (Boolean.TRUE.equals(locked)) {
                    LOTS.forEach(this::ensureRunning);
                }
            });
        } catch (RuntimeException e) {
            log.warn("Demo seeding failed, will retry: {}", e.toString());
        }
    }

    private void ensureRunning(Lot lot) {
        boolean needed = Boolean.TRUE.equals(jdbc.sql("""
                        SELECT NOT EXISTS (
                            SELECT 1 FROM auctions a
                             WHERE a.demo_key = :key
                               AND (a.status IN ('SCHEDULED', 'OPEN')
                                    OR a.closed_at IS NULL
                                    OR a.closed_at > clock_timestamp() - :delayMs * interval '1 millisecond'))
                        """)
                .param("key", lot.key()).param("delayMs", reopenDelay.toMillis())
                .query(Boolean.class).single());
        if (needed) {
            auctions.create(new NewAuction.Builder()
                    .title(lot.title()).description(lot.description()).seller(users.seller().id())
                    .demoKey(lot.key()).startPrice(lot.startPrice()).minIncrement(lot.increment())
                    .reservePrice(lot.reserve()).duration(lot.duration())
                    .antiSniping(ANTI_SNIPE_SECONDS, MAX_EXTENSIONS).build());
            log.info("Demo lot '{}' scheduled", lot.key());
        }
    }
}
