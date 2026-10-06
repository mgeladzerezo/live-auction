package io.github.mgeladzerezo.auction.bid;

import io.github.mgeladzerezo.auction.auction.AuctionRepository;
import io.github.mgeladzerezo.auction.auction.AuctionStatus;
import io.github.mgeladzerezo.auction.bid.BidRules.Decision;
import io.github.mgeladzerezo.auction.event.AuctionEvent;
import io.github.mgeladzerezo.auction.event.EventStore;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * One bid attempt as one database transaction. This is the only code that writes bids.
 *
 * <p>The transaction reads the auction row together with the database clock, lets
 * {@link BidRules} decide, and then either records a rejection or applies the acceptance:
 * conditional update of the auction row, attempt row, bid row and events, all or nothing.
 *
 * <p>Two things make the decision safe to write:
 * <ul>
 *   <li><b>The version predicate.</b> The auction is updated with {@code WHERE version = ?}.
 *       If any other transaction changed the row since it was read (another bid, an extension,
 *       the close), no row matches, nothing is written and the caller gets
 *       {@link Attempt.VersionConflict}. If it does match, the row is exactly the one the
 *       decision was made on, so the decision, including "the deadline had not passed at
 *       {@code dbNow}" and "this bid extends the deadline", is still right.</li>
 *   <li><b>The attempt key.</b> {@code bid_attempts} has primary key
 *       {@code (bidder_id, client_bid_id)}. The insert uses {@code ON CONFLICT DO NOTHING};
 *       if the key already exists the whole transaction is rolled back and the stored answer
 *       is returned instead. A re-sent bid therefore gets the original answer and can never
 *       create a second bid, whatever the state of the auction is by then.</li>
 * </ul>
 *
 * <p>The locking strategies are thin wrappers around {@link #attempt}: the optimistic one calls
 * it with {@link LockMode#NONE} and retries on conflict, the pessimistic one calls it once with
 * {@link LockMode#FOR_UPDATE}.
 */
@Component
public class BidLedger {

    /** How the auction row is read at the start of the attempt. */
    public enum LockMode {
        /** Plain read; correctness comes from the version predicate on the write. */
        NONE,
        /** {@code SELECT ... FOR UPDATE}; the row lock is held until commit. */
        FOR_UPDATE
    }

    /** What one transaction achieved. */
    public sealed interface Attempt {

        /** The bid has its definitive answer and the answer is committed. */
        record Done(BidResult result) implements Attempt {
        }

        /** The auction row changed between read and write; nothing was written. */
        record VersionConflict() implements Attempt {
        }
    }

    private static final String STATE_COLUMNS = """
            id, status, seller_id, start_price, min_increment, current_price, leader_id, ends_at,
            anti_snipe_window_seconds, max_extensions, extension_count, bid_count, version, last_seq
            """;

    private static final String READ_UNLOCKED =
            "SELECT " + STATE_COLUMNS + ", clock_timestamp() AS db_now FROM auctions WHERE id = :id";

    /**
     * The clock is read in the outer query, over a materialised CTE, so that it is evaluated
     * after the row lock has been granted. Reading it in the locking SELECT itself could return
     * a time from before a long lock wait.
     */
    private static final String READ_LOCKED = """
            WITH locked AS MATERIALIZED (
                SELECT %s FROM auctions WHERE id = :id FOR UPDATE)
            SELECT locked.*, clock_timestamp() AS db_now FROM locked
            """.formatted(STATE_COLUMNS);

    private final JdbcClient jdbc;
    private final EventStore events;
    private final TransactionTemplate transaction;
    private volatile BidCheckpoint checkpoint = BidCheckpoint.NONE;

    public BidLedger(JdbcClient jdbc, EventStore events, PlatformTransactionManager transactionManager) {
        this.jdbc = jdbc;
        this.events = events;
        this.transaction = new TransactionTemplate(transactionManager);
    }

    /** Test seam, see {@link BidCheckpoint}. */
    public void installCheckpoint(BidCheckpoint checkpoint) {
        this.checkpoint = checkpoint == null ? BidCheckpoint.NONE : checkpoint;
    }

    /** Runs one read-decide-write transaction for the bid. */
    public Attempt attempt(BidCommand command, LockMode lockMode) {
        return transaction.execute(status -> {
            Optional<BidState> found = readState(command.auctionId(), lockMode);
            if (found.isEmpty()) {
                return reject(status, command, RejectReason.AUCTION_NOT_FOUND, null);
            }
            BidState state = found.get();
            Decision decision = BidRules.decide(state, command.bidderId(), command.amount());
            checkpoint.afterRead(command);
            return switch (decision) {
                case Decision.Reject(RejectReason reason) -> reject(status, command, reason, state);
                case Decision.Accept accept -> accept(status, command, state, accept);
            };
        });
    }

    /**
     * Records the definitive CONTENTION rejection once the optimistic strategy has given up.
     * Like every answer it is keyed by the client id, so it is as idempotent as the others.
     */
    public BidResult rejectAfterContention(BidCommand command) {
        Attempt.Done done = transaction.execute(
                status -> reject(status, command, RejectReason.CONTENTION, null));
        return done.result();
    }

    private Optional<BidState> readState(long auctionId, LockMode lockMode) {
        return jdbc.sql(lockMode == LockMode.FOR_UPDATE ? READ_LOCKED : READ_UNLOCKED)
                .param("id", auctionId)
                .query(BidLedger::mapState)
                .optional();
    }

    private Attempt.Done reject(TransactionStatus status, BidCommand command, RejectReason reason, BidState state) {
        if (!recordAttempt(command, "REJECTED", reason, state == null ? null : state.dbNow())) {
            return duplicate(status, command);
        }
        return new Attempt.Done(new BidResult.Rejected(command.clientBidId(), command.auctionId(),
                command.amount(), reason, state == null ? null : state.currentPrice(),
                state == null ? null : state.minimumBid(), false));
    }

    private Attempt accept(TransactionStatus status, BidCommand command, BidState state, Decision.Accept accept) {
        int updated = jdbc.sql("""
                        UPDATE auctions
                           SET current_price = :amount,
                               leader_id = :bidderId,
                               bid_count = bid_count + 1,
                               ends_at = :endsAt,
                               extension_count = extension_count + :extensions,
                               version = version + 1,
                               last_seq = last_seq + :events
                         WHERE id = :id
                           AND version = :version
                        """)
                .param("amount", command.amount())
                .param("bidderId", command.bidderId())
                .param("endsAt", accept.endsAt().atOffset(ZoneOffset.UTC))
                .param("extensions", accept.extended() ? 1 : 0)
                .param("events", accept.extended() ? 2 : 1)
                .param("id", state.auctionId())
                .param("version", state.version())
                .update();
        if (updated == 0) {
            status.setRollbackOnly();
            return new Attempt.VersionConflict();
        }
        if (!recordAttempt(command, "ACCEPTED", null, state.dbNow())) {
            // Same client id answered before: undo the auction update and return that answer.
            return duplicate(status, command);
        }

        long bidSeq = state.lastSeq() + 1;
        Instant extendedTo = accept.extended() ? accept.endsAt() : null;
        long bidId = jdbc.sql("""
                        INSERT INTO bids (auction_id, bidder_id, client_bid_id, amount, seq, accepted_at, extended_to)
                        VALUES (:auctionId, :bidderId, :clientBidId, :amount, :seq, :acceptedAt,
                                CAST(:extendedTo AS timestamptz))
                        RETURNING id
                        """)
                .param("auctionId", state.auctionId())
                .param("bidderId", command.bidderId())
                .param("clientBidId", command.clientBidId())
                .param("amount", command.amount())
                .param("seq", bidSeq)
                .param("acceptedAt", state.dbNow().atOffset(ZoneOffset.UTC))
                .param("extendedTo", extendedTo == null ? null : extendedTo.atOffset(ZoneOffset.UTC))
                .query(Long.class)
                .single();

        events.append(new AuctionEvent.BidAccepted(state.auctionId(), bidSeq, state.dbNow(), bidId,
                command.amount(), command.bidderId(), command.bidderName(), state.bidCount() + 1,
                command.amount() + state.minIncrement(), accept.endsAt()));
        if (accept.extended()) {
            events.append(new AuctionEvent.TimeExtended(state.auctionId(), bidSeq + 1, state.dbNow(),
                    state.endsAt(), accept.endsAt(), state.extensionCount() + 1, state.maxExtensions()));
        }
        checkpoint.beforeCommit(command);
        return new Attempt.Done(new BidResult.Accepted(command.clientBidId(), state.auctionId(),
                command.amount(), bidId, bidSeq, state.dbNow(), extendedTo, false));
    }

    /** @return false if an answer for this client id already exists */
    private boolean recordAttempt(BidCommand command, String outcome, RejectReason reason, Instant decidedAt) {
        return jdbc.sql("""
                        INSERT INTO bid_attempts (bidder_id, client_bid_id, auction_id, amount, outcome, reason, decided_at)
                        VALUES (:bidderId, :clientBidId, :auctionId, :amount, :outcome, CAST(:reason AS text),
                                COALESCE(CAST(:decidedAt AS timestamptz), clock_timestamp()))
                        ON CONFLICT (bidder_id, client_bid_id) DO NOTHING
                        """)
                .param("bidderId", command.bidderId())
                .param("clientBidId", command.clientBidId())
                .param("auctionId", command.auctionId())
                .param("amount", command.amount())
                .param("outcome", outcome)
                .param("reason", reason == null ? null : reason.name())
                .param("decidedAt", decidedAt == null ? null : decidedAt.atOffset(ZoneOffset.UTC))
                .update() == 1;
    }

    /**
     * Rolls back whatever this transaction wrote and answers with the stored result. By the
     * time {@code ON CONFLICT DO NOTHING} reports a conflict the other transaction has
     * committed, so the row is visible to this read.
     */
    private Attempt.Done duplicate(TransactionStatus status, BidCommand command) {
        status.setRollbackOnly();
        BidResult stored = jdbc.sql("""
                        SELECT t.client_bid_id, t.auction_id, t.amount, t.outcome, t.reason,
                               b.id AS bid_id, b.seq, b.accepted_at, b.extended_to
                          FROM bid_attempts t
                          LEFT JOIN bids b ON b.bidder_id = t.bidder_id AND b.client_bid_id = t.client_bid_id
                         WHERE t.bidder_id = :bidderId AND t.client_bid_id = :clientBidId
                        """)
                .param("bidderId", command.bidderId())
                .param("clientBidId", command.clientBidId())
                .query(BidLedger::mapStored)
                .optional()
                .orElseThrow(() -> new IllegalStateException(
                        "Attempt conflicted but no stored answer found for " + command.clientBidId()));
        return new Attempt.Done(stored);
    }

    private static BidResult mapStored(ResultSet rs, int row) throws SQLException {
        if ("ACCEPTED".equals(rs.getString("outcome"))) {
            return new BidResult.Accepted(rs.getString("client_bid_id"), rs.getLong("auction_id"),
                    rs.getLong("amount"), rs.getLong("bid_id"), rs.getLong("seq"),
                    AuctionRepository.instant(rs, "accepted_at"), AuctionRepository.instant(rs, "extended_to"), true);
        }
        return new BidResult.Rejected(rs.getString("client_bid_id"), rs.getLong("auction_id"),
                rs.getLong("amount"), RejectReason.valueOf(rs.getString("reason")), null, null, true);
    }

    private static BidState mapState(ResultSet rs, int row) throws SQLException {
        return new BidState(
                rs.getLong("id"),
                AuctionStatus.valueOf(rs.getString("status")),
                rs.getObject("seller_id", Long.class),
                rs.getLong("start_price"),
                rs.getLong("min_increment"),
                rs.getObject("current_price", Long.class),
                rs.getObject("leader_id", Long.class),
                AuctionRepository.instant(rs, "ends_at"),
                rs.getInt("anti_snipe_window_seconds"),
                rs.getInt("max_extensions"),
                rs.getInt("extension_count"),
                rs.getInt("bid_count"),
                rs.getLong("version"),
                rs.getLong("last_seq"),
                AuctionRepository.instant(rs, "db_now"));
    }
}
