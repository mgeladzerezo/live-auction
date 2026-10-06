package io.github.mgeladzerezo.auction.event;

import io.github.mgeladzerezo.auction.auction.AuctionRepository;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

/**
 * Append-only event log, and the outbox that feeds every application instance.
 *
 * <p>{@link #append} must run inside the transaction that changes the auction. It inserts the
 * event row and issues {@code pg_notify} in a single statement. PostgreSQL queues a
 * notification until the transaction commits and discards it on rollback, so "broadcast only
 * after commit" is a property of the database here, not of careful ordering in Java, and it
 * holds on every instance that listens.
 */
@Repository
public class EventStore {

    /** Channel every application instance listens on. */
    public static final String CHANNEL = "auction_events";

    /** PostgreSQL rejects NOTIFY payloads of 8000 bytes or more. */
    static final int MAX_NOTIFY_BYTES = 7900;

    private final JdbcClient jdbc;
    private final JsonMapper json;

    public EventStore(JdbcClient jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    /**
     * Stores the event and schedules its notification for commit time.
     *
     * @throws IllegalStateException if called outside a transaction or if the event would not
     *                               fit in a notification
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void append(AuctionEvent event) {
        String payload = json.writeValueAsString(event);
        if (payload.getBytes(StandardCharsets.UTF_8).length > MAX_NOTIFY_BYTES) {
            throw new IllegalStateException("Event too large for NOTIFY: " + event.typeName());
        }
        jdbc.sql("""
                        WITH stored AS (
                            INSERT INTO auction_events (auction_id, seq, type, payload, created_at)
                            VALUES (:auctionId, :seq, :type, CAST(:payload AS jsonb), :at)
                            RETURNING 1)
                        SELECT pg_notify(:channel, :payload) FROM stored
                        """)
                .param("auctionId", event.auctionId())
                .param("seq", event.seq())
                .param("type", event.typeName())
                .param("payload", payload)
                .param("at", event.at().atOffset(ZoneOffset.UTC))
                .param("channel", CHANNEL)
                .query((rs, row) -> 1)
                .single();
    }

    /** Events with a sequence number above {@code afterSeq}, oldest first. */
    public List<StoredEvent> after(long auctionId, long afterSeq, int limit) {
        return jdbc.sql("""
                        SELECT auction_id, seq, created_at, payload::text AS payload
                          FROM auction_events
                         WHERE auction_id = :auctionId AND seq > :afterSeq
                         ORDER BY seq
                         LIMIT :limit
                        """)
                .param("auctionId", auctionId)
                .param("afterSeq", afterSeq)
                .param("limit", limit)
                .query((rs, row) -> new StoredEvent(rs.getLong("auction_id"), rs.getLong("seq"),
                        AuctionRepository.instant(rs, "created_at"), rs.getString("payload")))
                .list();
    }
}
