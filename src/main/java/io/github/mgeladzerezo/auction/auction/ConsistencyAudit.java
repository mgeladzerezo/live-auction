package io.github.mgeladzerezo.auction.auction;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * Re-derives an auction's state from its append-only bid history and compares it with the
 * denormalised columns on the auction row.
 *
 * <p>The auction row carries {@code current_price}, {@code leader_id}, {@code bid_count},
 * {@code ends_at} and {@code extension_count} so that the hot path never has to aggregate. That
 * is only acceptable if the copy provably cannot drift, so this audit replays the history from
 * scratch: prices and increments, the leader, the deadline in force when each bid was decided,
 * every anti-sniping extension, and the event sequence. Settlement refuses to finalise an
 * auction that fails it, and the load tests run it after every storm.
 */
@Component
public class ConsistencyAudit {

    /**
     * @param violations human-readable descriptions of every rule the data breaks; empty when
     *                   the auction is consistent
     */
    public record Report(long auctionId, int bids, long events, List<String> violations) {

        @JsonProperty
        public boolean consistent() {
            return violations.isEmpty();
        }
    }

    private record BidRow(long id, long bidderId, long amount, long seq, Instant acceptedAt, Instant extendedTo) {
    }

    private final JdbcClient jdbc;
    private final AuctionRepository auctions;

    public ConsistencyAudit(JdbcClient jdbc, AuctionRepository auctions) {
        this.jdbc = jdbc;
        this.auctions = auctions;
    }

    /** Audits one auction. Safe to call at any time; reads only committed data. */
    public Report check(long auctionId) {
        Auction auction = auctions.find(auctionId)
                .orElseThrow(() -> new IllegalArgumentException("No auction " + auctionId));
        List<BidRow> bids = jdbc.sql("""
                        SELECT id, bidder_id, amount, seq, accepted_at, extended_to
                          FROM bids WHERE auction_id = :id ORDER BY seq
                        """)
                .param("id", auctionId)
                .query((rs, row) -> new BidRow(rs.getLong("id"), rs.getLong("bidder_id"), rs.getLong("amount"),
                        rs.getLong("seq"), AuctionRepository.instant(rs, "accepted_at"),
                        AuctionRepository.instant(rs, "extended_to")))
                .list();

        List<String> violations = new ArrayList<>();
        replayBids(auction, bids, violations);
        compareHead(auction, bids, violations);
        long events = checkEvents(auction, bids, violations);
        checkAttempts(auctionId, bids.size(), violations);
        return new Report(auctionId, bids.size(), events, List.copyOf(violations));
    }

    /** Walks the history in order, re-applying the price rules and the anti-sniping rule. */
    private static void replayBids(Auction auction, List<BidRow> bids, List<String> violations) {
        Duration window = Duration.ofSeconds(auction.antiSnipeWindowSeconds());
        Instant deadline = auction.originalEndsAt();
        int extensions = 0;
        BidRow previous = null;
        for (BidRow bid : bids) {
            long minimum = previous == null ? auction.startPrice() : previous.amount() + auction.minIncrement();
            if (bid.amount() < minimum) {
                violations.add("bid %d of %d is below the minimum %d in force".formatted(bid.id(), bid.amount(), minimum));
            }
            if (previous != null && previous.bidderId() == bid.bidderId()) {
                violations.add("bid %d outbids the same bidder's own leading bid".formatted(bid.id()));
            }
            if (bid.acceptedAt().isBefore(auction.startsAt())) {
                violations.add("bid %d was accepted before the auction started".formatted(bid.id()));
            }
            if (!bid.acceptedAt().isBefore(deadline)) {
                violations.add("bid %d was accepted at %s, not before the deadline %s in force"
                        .formatted(bid.id(), bid.acceptedAt(), deadline));
            }
            boolean shouldExtend = auction.antiSnipeWindowSeconds() > 0
                    && extensions < auction.maxExtensions()
                    && Duration.between(bid.acceptedAt(), deadline).compareTo(window) <= 0;
            Instant expectedExtension = shouldExtend ? deadline.plus(window) : null;
            if (!Objects.equals(expectedExtension, bid.extendedTo())) {
                violations.add("bid %d extended the deadline to %s, expected %s"
                        .formatted(bid.id(), bid.extendedTo(), expectedExtension));
            }
            if (shouldExtend) {
                deadline = expectedExtension;
                extensions++;
            }
            previous = bid;
        }
        if (!deadline.equals(auction.endsAt())) {
            violations.add("end time is %s but the bid history implies %s".formatted(auction.endsAt(), deadline));
        }
        if (extensions != auction.extensionCount()) {
            violations.add("extension_count is %d but the bid history implies %d"
                    .formatted(auction.extensionCount(), extensions));
        }
    }

    /** The denormalised head on the auction row must equal the last bid. */
    private static void compareHead(Auction auction, List<BidRow> bids, List<String> violations) {
        BidRow last = bids.isEmpty() ? null : bids.getLast();
        Long expectedPrice = last == null ? null : last.amount();
        Long expectedLeader = last == null ? null : last.bidderId();
        if (!Objects.equals(expectedPrice, auction.currentPrice())) {
            violations.add("current_price is %s but the last bid is %s".formatted(auction.currentPrice(), expectedPrice));
        }
        if (!Objects.equals(expectedLeader, auction.leaderId())) {
            violations.add("leader_id is %s but the last bidder is %s".formatted(auction.leaderId(), expectedLeader));
        }
        if (bids.size() != auction.bidCount()) {
            violations.add("bid_count is %d but there are %d bids".formatted(auction.bidCount(), bids.size()));
        }
    }

    /** Event numbers must be exactly 1..last_seq and agree with the bid history. */
    private long checkEvents(Auction auction, List<BidRow> bids, List<String> violations) {
        record EventStats(long count, long maxSeq, long bidEvents, long extensionEvents, long unmatchedBids) {
        }
        EventStats stats = jdbc.sql("""
                        SELECT count(*) AS total,
                               coalesce(max(e.seq), 0) AS max_seq,
                               count(*) FILTER (WHERE e.type = 'BID_ACCEPTED') AS bid_events,
                               count(*) FILTER (WHERE e.type = 'TIME_EXTENDED') AS extension_events,
                               (SELECT count(*) FROM bids b
                                 WHERE b.auction_id = :id
                                   AND NOT EXISTS (SELECT 1 FROM auction_events x
                                                    WHERE x.auction_id = b.auction_id AND x.seq = b.seq
                                                      AND x.type = 'BID_ACCEPTED'
                                                      AND (x.payload ->> 'bidId')::bigint = b.id
                                                      AND (x.payload ->> 'price')::bigint = b.amount)) AS unmatched
                          FROM auction_events e
                         WHERE e.auction_id = :id
                        """)
                .param("id", auction.id())
                .query((rs, row) -> new EventStats(rs.getLong("total"), rs.getLong("max_seq"),
                        rs.getLong("bid_events"), rs.getLong("extension_events"), rs.getLong("unmatched")))
                .single();
        if (stats.count() != auction.lastSeq() || stats.maxSeq() != auction.lastSeq()) {
            violations.add("event log has %d events up to seq %d but last_seq is %d"
                    .formatted(stats.count(), stats.maxSeq(), auction.lastSeq()));
        }
        if (stats.bidEvents() != bids.size()) {
            violations.add("%d BID_ACCEPTED events for %d bids".formatted(stats.bidEvents(), bids.size()));
        }
        if (stats.extensionEvents() != auction.extensionCount()) {
            violations.add("%d TIME_EXTENDED events for extension_count %d"
                    .formatted(stats.extensionEvents(), auction.extensionCount()));
        }
        if (stats.unmatchedBids() != 0) {
            violations.add("%d bids have no matching BID_ACCEPTED event".formatted(stats.unmatchedBids()));
        }
        return stats.count();
    }

    /** Exactly the accepted attempts have a bid row. */
    private void checkAttempts(long auctionId, int bids, List<String> violations) {
        long acceptedAttempts = jdbc.sql(
                        "SELECT count(*) FROM bid_attempts WHERE auction_id = :id AND outcome = 'ACCEPTED'")
                .param("id", auctionId)
                .query(Long.class)
                .single();
        if (acceptedAttempts != bids) {
            violations.add("%d ACCEPTED attempts but %d bids".formatted(acceptedAttempts, bids));
        }
    }
}
