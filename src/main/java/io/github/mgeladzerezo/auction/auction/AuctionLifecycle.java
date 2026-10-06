package io.github.mgeladzerezo.auction.auction;

import io.github.mgeladzerezo.auction.config.AuctionProperties;
import io.github.mgeladzerezo.auction.event.AuctionEvent;
import io.github.mgeladzerezo.auction.event.EventStore;
import io.github.mgeladzerezo.auction.metrics.AuctionMetrics;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Moves auctions through their lifecycle: opens the ones whose start time has come, closes the
 * ones whose end time has passed, and settles the closed ones.
 *
 * <p><b>Whose clock.</b> "Has the end time passed?" is answered by the database
 * ({@code statement_timestamp()}), the same clock bids are judged against. Application
 * instances can disagree with each other and with the database by seconds; a single clock that
 * every transaction already talks to cannot disagree with itself.
 *
 * <p><b>Several instances.</b> Each transition claims its rows with
 * {@code FOR UPDATE SKIP LOCKED}: two schedulers never process the same auction, and neither
 * waits for the other. The claim and the status change are one statement, so there is no gap
 * between "found it due" and "marked it".
 *
 * <p><b>The race with a last-instant bid.</b> Both the close and a bid must take the auction's
 * row lock to write, so one of them goes first:
 * <ul>
 *   <li>Bid first. If it extended the deadline, the close re-checks its {@code WHERE} against
 *       the new row version (PostgreSQL does this for a row that changed after the statement's
 *       snapshot) and no longer finds {@code ends_at} in the past: the close loses. If the bid
 *       did not extend, the auction closes with that bid as the winner. If the bid is still
 *       uncommitted, {@code SKIP LOCKED} passes over the row and the next tick looks again.</li>
 *   <li>Close first. It increments {@code version}. An optimistic bid's
 *       {@code WHERE version = ?} then matches nothing, it retries, reads CLOSED and is
 *       rejected. A pessimistic bid waits for the lock, reads CLOSED and is rejected. Either
 *       way the bid loses.</li>
 * </ul>
 */
@Service
public class AuctionLifecycle {

    private static final Logger log = LoggerFactory.getLogger(AuctionLifecycle.class);

    private final JdbcClient jdbc;
    private final EventStore events;
    private final ConsistencyAudit audit;
    private final AuctionMetrics metrics;
    private final TransactionTemplate transaction;
    private final int batchSize;

    public AuctionLifecycle(JdbcClient jdbc, EventStore events, ConsistencyAudit audit, AuctionMetrics metrics,
                            PlatformTransactionManager transactionManager,
                            AuctionProperties properties) {
        this.jdbc = jdbc;
        this.events = events;
        this.audit = audit;
        this.metrics = metrics;
        this.transaction = new TransactionTemplate(transactionManager);
        this.batchSize = properties.lifecycle().batchSize();
    }

    /** One scheduler pass. Each transition is its own transaction. */
    public void tick() {
        openDue();
        closeDue();
        settleClosed();
    }

    /** SCHEDULED to OPEN for auctions whose start time has come. Returns how many were opened. */
    public int openDue() {
        int opened = transaction.execute(status -> {
            List<AuctionEvent.Opened> openedEvents = jdbc.sql("""
                            UPDATE auctions a
                               SET status = 'OPEN', version = a.version + 1, last_seq = a.last_seq + 1
                              FROM (SELECT id FROM auctions
                                     WHERE status = 'SCHEDULED' AND starts_at <= statement_timestamp()
                                     ORDER BY starts_at
                                     LIMIT :batch
                                       FOR UPDATE SKIP LOCKED) due
                             WHERE a.id = due.id
                            RETURNING a.id, a.last_seq, a.ends_at, statement_timestamp() AS at
                            """)
                    .param("batch", batchSize)
                    .query((rs, row) -> new AuctionEvent.Opened(rs.getLong("id"), rs.getLong("last_seq"),
                            AuctionRepository.instant(rs, "at"), AuctionRepository.instant(rs, "ends_at")))
                    .list();
            openedEvents.forEach(events::append);
            return openedEvents.size();
        });
        metrics.lifecycleTransition("OPEN", opened);
        return opened;
    }

    /** OPEN to CLOSED for auctions whose end time has passed. Returns how many were closed. */
    public int closeDue() {
        int closed = transaction.execute(status -> {
            List<AuctionEvent.Closed> closedEvents = jdbc.sql("""
                            UPDATE auctions a
                               SET status = 'CLOSED', closed_at = statement_timestamp(),
                                   version = a.version + 1, last_seq = a.last_seq + 1
                              FROM (SELECT id FROM auctions
                                     WHERE status = 'OPEN' AND ends_at <= statement_timestamp()
                                     ORDER BY ends_at
                                     LIMIT :batch
                                       FOR UPDATE SKIP LOCKED) due
                             WHERE a.id = due.id
                            RETURNING a.id, a.last_seq, a.closed_at, a.ends_at, a.current_price, a.leader_id,
                                      a.reserve_price, a.bid_count,
                                      (SELECT u.username FROM users u WHERE u.id = a.leader_id) AS leader_name
                            """)
                    .param("batch", batchSize)
                    .query((rs, row) -> {
                        Long price = rs.getObject("current_price", Long.class);
                        boolean reserveMet = Auction.reserveMet(price, rs.getObject("reserve_price", Long.class));
                        return new AuctionEvent.Closed(rs.getLong("id"), rs.getLong("last_seq"),
                                AuctionRepository.instant(rs, "closed_at"),
                                reserveMet ? rs.getObject("leader_id", Long.class) : null,
                                reserveMet ? rs.getString("leader_name") : null,
                                price, reserveMet, rs.getInt("bid_count"),
                                AuctionRepository.instant(rs, "ends_at"));
                    })
                    .list();
            closedEvents.forEach(events::append);
            return closedEvents.size();
        });
        metrics.lifecycleTransition("CLOSED", closed);
        return closed;
    }

    /**
     * CLOSED to SETTLED or UNSOLD, after the audit has re-derived the result from the bid
     * history. An auction that fails the audit stays CLOSED with the reason recorded; it is
     * not retried automatically, because a wrong winner is worse than a late one.
     */
    public int settleClosed() {
        int settled = transaction.execute(status -> {
            List<Long> claimed = jdbc.sql("""
                            SELECT id FROM auctions
                             WHERE status = 'CLOSED' AND settle_error IS NULL
                             ORDER BY id
                             LIMIT :batch
                               FOR UPDATE SKIP LOCKED
                            """)
                    .param("batch", batchSize)
                    .query(Long.class)
                    .list();
            int done = 0;
            for (long auctionId : claimed) {
                done += settle(auctionId) ? 1 : 0;
            }
            return done;
        });
        metrics.lifecycleTransition("SETTLED_OR_UNSOLD", settled);
        return settled;
    }

    private boolean settle(long auctionId) {
        ConsistencyAudit.Report report = audit.check(auctionId);
        if (!report.consistent()) {
            log.error("Auction {} failed its settlement audit and stays CLOSED: {}", auctionId, report.violations());
            metrics.settlementBlocked();
            jdbc.sql("UPDATE auctions SET settle_error = :error, version = version + 1 WHERE id = :id")
                    .param("error", String.join("; ", report.violations()))
                    .param("id", auctionId)
                    .update();
            return false;
        }
        AuctionEvent.Settled settled = jdbc.sql("""
                        UPDATE auctions a
                           SET status = CASE WHEN a.current_price IS NOT NULL
                                              AND (a.reserve_price IS NULL OR a.current_price >= a.reserve_price)
                                             THEN 'SETTLED' ELSE 'UNSOLD' END,
                               version = a.version + 1, last_seq = a.last_seq + 1
                         WHERE a.id = :id
                        RETURNING a.id, a.last_seq, a.status, a.current_price, a.leader_id,
                                  statement_timestamp() AS at,
                                  (SELECT u.username FROM users u WHERE u.id = a.leader_id) AS leader_name
                        """)
                .param("id", auctionId)
                .query((rs, row) -> {
                    AuctionStatus finalStatus = AuctionStatus.valueOf(rs.getString("status"));
                    boolean sold = finalStatus == AuctionStatus.SETTLED;
                    Instant at = AuctionRepository.instant(rs, "at");
                    return new AuctionEvent.Settled(rs.getLong("id"), rs.getLong("last_seq"), at, finalStatus,
                            sold ? rs.getObject("leader_id", Long.class) : null,
                            sold ? rs.getString("leader_name") : null,
                            rs.getObject("current_price", Long.class));
                })
                .single();
        events.append(settled);
        return true;
    }
}
