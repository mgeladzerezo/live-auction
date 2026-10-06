package io.github.mgeladzerezo.auction.realtime;

import io.github.mgeladzerezo.auction.auction.Auction;
import io.github.mgeladzerezo.auction.auction.AuctionRepository;
import io.github.mgeladzerezo.auction.auction.AuctionView;
import io.github.mgeladzerezo.auction.bid.BidHistory;
import io.github.mgeladzerezo.auction.config.AuctionProperties;
import io.github.mgeladzerezo.auction.config.DbClock;
import io.github.mgeladzerezo.auction.event.EventFanout;
import io.github.mgeladzerezo.auction.event.EventStore;
import io.github.mgeladzerezo.auction.event.StoredEvent;
import io.github.mgeladzerezo.auction.metrics.AuctionMetrics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * This instance's subscribers, grouped by auction, and the fan-out of events to them.
 *
 * <p>Events reach the hub from exactly one place, the LISTEN connection
 * ({@link io.github.mgeladzerezo.auction.event.NotificationListener}), whether the bid was
 * accepted on this instance or on another one. There is deliberately no shortcut that
 * broadcasts locally after a commit: one path means one ordering, and a single-instance
 * deployment exercises the same code as a clustered one.
 *
 * <p>The hub never trusts the notification stream to be complete. Subscriptions check sequence
 * numbers and are re-read from the event log when a number is skipped
 * ({@link #synchronise}); a periodic sweep catches the case where the stream simply stops.
 */
@Component
public class AuctionHub implements EventFanout {

    private static final Logger log = LoggerFactory.getLogger(AuctionHub.class);
    private static final int MAX_SYNC_ROUNDS = 3;

    private final Map<Long, Set<Subscription>> rooms = new ConcurrentHashMap<>();
    private final AuctionRepository auctions;
    private final BidHistory bids;
    private final EventStore events;
    private final JdbcClient jdbc;
    private final JsonMapper json;
    private final DbClock clock;
    private final AuctionMetrics metrics;
    private final AuctionProperties.Realtime settings;
    /** {@code last_seq} per watched auction as seen by the previous sweep. */
    private Map<Long, Long> previousSweep = Map.of();

    public AuctionHub(AuctionRepository auctions, BidHistory bids, EventStore events, JdbcClient jdbc,
                      JsonMapper json, DbClock clock, AuctionMetrics metrics, AuctionProperties properties) {
        this.auctions = auctions;
        this.bids = bids;
        this.events = events;
        this.jdbc = jdbc;
        this.json = json;
        this.clock = clock;
        this.metrics = metrics;
        this.settings = properties.realtime();
        metrics.gauge("auction.ws.subscriptions", "Auction subscriptions held by this instance",
                () -> rooms.values().stream().mapToInt(Set::size).sum());
    }

    /**
     * Starts delivering an auction's events to a connection.
     *
     * @param lastSeq the last sequence number the client already has, or null for a fresh start.
     *                With it the client is replayed exactly the events it missed; without it
     *                (or if it is too far behind) it gets a full snapshot.
     */
    public void subscribe(ClientConnection connection, long auctionId, Long lastSeq) {
        if (!connection.subscriptions().containsKey(auctionId)
                && connection.subscriptions().size() >= settings.maxSubscriptionsPerSession()) {
            connection.send(json.writeValueAsString(new ServerMessage.Failure("TOO_MANY_SUBSCRIPTIONS",
                    "At most " + settings.maxSubscriptionsPerSession() + " auctions per connection",
                    null, auctionId, false)));
            return;
        }
        Subscription subscription = new Subscription(connection, auctionId);
        Subscription replaced = connection.subscriptions().put(auctionId, subscription);
        // Register before reading the snapshot: from here on no event can be missed, only buffered.
        rooms.compute(auctionId, (id, room) -> {
            Set<Subscription> members = room == null ? ConcurrentHashMap.newKeySet() : room;
            if (replaced != null) {
                members.remove(replaced);
            }
            members.add(subscription);
            return members;
        });
        synchronise(subscription, lastSeq);
    }

    public void unsubscribe(ClientConnection connection, long auctionId) {
        Subscription subscription = connection.subscriptions().remove(auctionId);
        if (subscription != null) {
            leave(subscription);
        }
    }

    /** Drops every subscription of a connection that has gone away. */
    public void disconnect(ClientConnection connection) {
        List<Subscription> held = new ArrayList<>(connection.subscriptions().values());
        connection.subscriptions().clear();
        held.forEach(this::leave);
    }

    /** Fans one event out to the local subscribers of its auction. Called by the listener thread. */
    @Override
    public void dispatch(StoredEvent event) {
        Set<Subscription> room = rooms.get(event.auctionId());
        if (room != null) {
            for (Subscription subscription : room) {
                if (subscription.deliver(event)) {
                    resynchroniseAsync(subscription);
                }
            }
        }
        metrics.broadcastLag(Duration.between(event.at(), clock.now()));
    }

    /**
     * Re-reads every subscription from the event log. Called when the LISTEN connection has
     * been re-established, because notifications sent while it was down are gone for good.
     */
    @Override
    public void resynchroniseAll() {
        rooms.values().forEach(room -> room.forEach(subscription -> {
            if (subscription.beginResync()) {
                resynchroniseAsync(subscription);
            }
        }));
    }

    /**
     * Safety net for a notification that never arrives. Compares what each live subscription
     * has delivered with the {@code last_seq} the <em>previous</em> sweep saw: an event that
     * old should have been pushed long ago, so anyone still behind it is resynchronised.
     */
    @Scheduled(fixedDelayString = "${auction.realtime.resync-interval:5s}")
    void sweep() {
        Map<Long, Long> expected = previousSweep;
        List<Long> watched = List.copyOf(rooms.keySet());
        if (watched.isEmpty()) {
            previousSweep = Map.of();
            return;
        }
        Map<Long, Long> current = new HashMap<>();
        jdbc.sql("SELECT id, last_seq FROM auctions WHERE id IN (:ids)")
                .param("ids", watched)
                .query(rs -> {
                    current.put(rs.getLong("id"), rs.getLong("last_seq"));
                });
        previousSweep = current;
        expected.forEach((auctionId, lastSeq) -> {
            Set<Subscription> room = rooms.get(auctionId);
            if (room == null) {
                return;
            }
            for (Subscription subscription : room) {
                if (subscription.isLive() && subscription.deliveredSeq() < lastSeq && subscription.beginResync()) {
                    log.warn("Subscription of {} to auction {} is behind the event log; resynchronising",
                            subscription.connection().id(), auctionId);
                    resynchroniseAsync(subscription);
                }
            }
        });
    }

    /** Number of auctions with at least one local subscriber. */
    public int watchedAuctions() {
        return rooms.size();
    }

    private void resynchroniseAsync(Subscription subscription) {
        metrics.subscriptionResynced();
        Thread.ofVirtual().name("ws-resync").start(() -> {
            try {
                synchronise(subscription, subscription.deliveredSeq());
            } catch (RuntimeException e) {
                log.warn("Resynchronising {} failed, closing it: {}", subscription.connection().id(), e.toString());
                subscription.connection().close(1011, "resynchronisation failed: reconnect");
            }
        });
    }

    /**
     * Brings a syncing subscription up to date from the database and switches it to live.
     * Sends a REPLAY of the missed events when the client is close behind, a SNAPSHOT otherwise.
     */
    private void synchronise(Subscription subscription, Long clientSeq) {
        long auctionId = subscription.auctionId();
        Long from = clientSeq;
        for (int round = 0; round < MAX_SYNC_ROUNDS; round++) {
            Optional<Auction> found = auctions.find(auctionId);
            if (found.isEmpty()) {
                unsubscribe(subscription.connection(), auctionId);
                subscription.connection().send(json.writeValueAsString(new ServerMessage.Failure(
                        "AUCTION_NOT_FOUND", "No auction " + auctionId, null, auctionId, false)));
                return;
            }
            Auction auction = found.get();
            boolean replayable = from != null && from >= 0 && from <= auction.lastSeq()
                    && auction.lastSeq() - from <= settings.replayLimit();
            boolean gapRemains;
            if (replayable) {
                List<StoredEvent> missed = events.after(auctionId, from, settings.replayLimit() * 2);
                long syncedSeq = missed.isEmpty() ? from : missed.getLast().seq();
                gapRemains = subscription.completeSync(syncedSeq, replayMessage(auctionId, from, syncedSeq, missed));
            } else {
                ServerMessage.Snapshot snapshot = new ServerMessage.Snapshot(auctionId, auction.lastSeq(),
                        clock.now(), AuctionView.of(auction),
                        bids.recent(auctionId, settings.snapshotBids(), auction.lastSeq()));
                gapRemains = subscription.completeSync(auction.lastSeq(), json.writeValueAsString(snapshot));
            }
            if (!gapRemains) {
                return;
            }
            from = subscription.deliveredSeq();
        }
        subscription.connection().close(1011, "could not synchronise: reconnect");
    }

    /** Builds the REPLAY batch around the stored event JSON without re-parsing it. */
    private String replayMessage(long auctionId, long fromSeq, long toSeq, List<StoredEvent> missed) {
        StringBuilder message = new StringBuilder(64 + missed.size() * 256);
        message.append("{\"type\":\"REPLAY\",\"auctionId\":").append(auctionId)
                .append(",\"fromSeq\":").append(fromSeq)
                .append(",\"seq\":").append(toSeq)
                .append(",\"serverTime\":\"").append(clock.now()).append("\",\"events\":[");
        for (int i = 0; i < missed.size(); i++) {
            if (i > 0) {
                message.append(',');
            }
            message.append(missed.get(i).json());
        }
        return message.append("]}").toString();
    }

    private void leave(Subscription subscription) {
        rooms.computeIfPresent(subscription.auctionId(), (id, room) -> {
            room.remove(subscription);
            return room.isEmpty() ? null : room;
        });
    }
}
