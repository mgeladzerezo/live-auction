package io.github.mgeladzerezo.auction.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Typed view of every {@code auction.*} setting. Defaults live here so the application runs
 * with no configuration beyond a datasource.
 *
 * @param instanceId name of this application instance, shown to clients so a two-instance
 *                   demo can tell which node a socket landed on
 */
@ConfigurationProperties("auction")
public record AuctionProperties(
        @DefaultValue("local") String instanceId,
        @DefaultValue Bidding bidding,
        @DefaultValue Lifecycle lifecycle,
        @DefaultValue Realtime realtime,
        @DefaultValue Auth auth,
        @DefaultValue Demo demo) {

    /**
     * @param strategy      {@code optimistic} or {@code pessimistic}
     * @param maxAttempts   how many times the optimistic strategy tries before answering CONTENTION
     * @param maxConcurrent bid transactions allowed to run at once on this instance; keep it
     *                      below the connection pool size so reads and the scheduler never starve
     * @param backoffBase   first backoff ceiling; doubles per failed attempt
     * @param backoffCap    upper bound for a single backoff sleep
     */
    public record Bidding(
            @DefaultValue("optimistic") String strategy,
            @DefaultValue("8") int maxAttempts,
            @DefaultValue("12") int maxConcurrent,
            @DefaultValue("2ms") Duration backoffBase,
            @DefaultValue("50ms") Duration backoffCap) {
    }

    /**
     * @param enabled   whether this instance runs the open/close/settle scheduler
     * @param tick      delay between scheduler passes
     * @param batchSize how many auctions one pass may claim per transition
     */
    public record Lifecycle(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("250ms") Duration tick,
            @DefaultValue("50") int batchSize) {
    }

    /**
     * @param sendQueueCapacity          messages a session may have queued before it is
     *                                   disconnected as a slow consumer
     * @param replayLimit                most missed events replayed on reconnect; beyond this the
     *                                   client gets a fresh snapshot instead
     * @param maxSubscriptionsPerSession cap on auctions one socket may follow
     * @param sendTimeout                how long a single blocking socket write may take
     * @param resyncInterval             period of the safety sweep that heals lost notifications
     * @param snapshotBids               number of recent bids included in a snapshot
     */
    public record Realtime(
            @DefaultValue("512") int sendQueueCapacity,
            @DefaultValue("400") int replayLimit,
            @DefaultValue("64") int maxSubscriptionsPerSession,
            @DefaultValue("5s") Duration sendTimeout,
            @DefaultValue("5s") Duration resyncInterval,
            @DefaultValue("25") int snapshotBids) {
    }

    /**
     * @param pbkdf2Iterations work factor for password hashing
     * @param tokenTtl         lifetime of a bearer token
     */
    public record Auth(
            @DefaultValue("210000") int pbkdf2Iterations,
            @DefaultValue("12h") Duration tokenTtl) {
    }

    /**
     * @param enabled     whether demo auctions are seeded and periodically reopened
     * @param reopenDelay pause between a demo auction finishing and its next run opening
     */
    public record Demo(
            @DefaultValue("false") boolean enabled,
            @DefaultValue("20s") Duration reopenDelay) {
    }
}
