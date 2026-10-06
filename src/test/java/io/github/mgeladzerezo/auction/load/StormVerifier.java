package io.github.mgeladzerezo.auction.load;

import io.github.mgeladzerezo.auction.auction.Auction;
import io.github.mgeladzerezo.auction.auction.AuctionRepository;
import io.github.mgeladzerezo.auction.auction.AuctionStatus;
import io.github.mgeladzerezo.auction.auction.ConsistencyAudit;
import io.github.mgeladzerezo.auction.load.StormBidder.Answer;
import io.github.mgeladzerezo.auction.load.StormBidder.Attempt;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Checks a finished storm from both ends: the answers and events the clients recorded, and the
 * rows in the database. Each check corresponds to one sentence of the "no bid is lost" claim.
 * Nothing here trusts the server's own bookkeeping (the denormalised auction row, its meters)
 * except to compare it with what is re-derived from the bid history.
 */
final class StormVerifier {

    /**
     * @param checks     what was verified, with the numbers involved, for the report
     * @param violations every broken invariant; empty when the run is clean
     */
    record Outcome(List<String> checks, List<String> violations, Map<Long, AuctionSummary> auctions) {
    }

    record AuctionSummary(long id, AuctionStatus status, int bids, long events, int extensions, Long finalPrice,
                          int subscribers) {
    }

    private record BidRow(long id, long auctionId, long bidderId, String clientBidId, long amount, long seq,
                          Instant acceptedAt, Instant extendedTo) {
    }

    private record AttemptRow(long auctionId, long amount, String outcome, String reason) {
    }

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final BidStorm.Scenario scenario;
    private final JdbcClient jdbc;
    private final AuctionRepository auctions;
    private final ConsistencyAudit audit;
    private final List<String> checks = new ArrayList<>();
    private final List<String> violations = new ArrayList<>();

    StormVerifier(BidStorm.Scenario scenario, JdbcClient jdbc, AuctionRepository auctions, ConsistencyAudit audit) {
        this.scenario = scenario;
        this.jdbc = jdbc;
        this.auctions = auctions;
        this.audit = audit;
    }

    Outcome verify(List<Long> auctionIds, List<StormBidder> bidders) {
        Map<Long, List<BidRow>> bidsByAuction = loadBids(auctionIds);
        everyAttemptHasExactlyOneAnswer(bidders);
        attemptLedgerMatchesTheClients(auctionIds, bidders);
        acceptedBidsAreExactlyTheBidsTable(bidders, bidsByAuction);
        Map<Long, AuctionSummary> summaries = new LinkedHashMap<>();
        for (long auctionId : auctionIds) {
            Auction auction = auctions.find(auctionId).orElseThrow();
            List<BidRow> bids = bidsByAuction.getOrDefault(auctionId, List.of());
            List<StormBidder> subscribers = bidders.stream().filter(b -> b.auctionId == auctionId).toList();
            pricesFormAValidLadder(auction, bids);
            finalStateEqualsTheLastAcceptedBid(auction, bids, subscribers);
            noBidIsTimestampedOutsideTheAuction(auction, bids);
            extensionsMatchTheBidHistory(auction, bids, subscribers);
            everySubscriberSawTheSameGapFreeStream(auction, subscribers);
            ConsistencyAudit.Report report = audit.check(auctionId);
            report.violations().forEach(v -> violations.add("auction " + auctionId + " server audit: " + v));
            summaries.put(auctionId, new AuctionSummary(auctionId, auction.status(), bids.size(), auction.lastSeq(),
                    auction.extensionCount(), auction.currentPrice(), subscribers.size()));
        }
        int extensionsExercised = summaries.values().stream().mapToInt(AuctionSummary::extensions).sum();
        if (extensionsExercised == 0) {
            violations.add("inconclusive run: no bid landed in an anti-sniping window, so extensions were never exercised");
        }
        for (StormBidder bidder : bidders) {
            bidder.problems.forEach(p -> violations.add("bidder " + bidder.index + ": " + p));
        }
        return new Outcome(List.copyOf(checks), List.copyOf(violations), summaries);
    }

    /** Every bid attempt got exactly one answer. */
    private void everyAttemptHasExactlyOneAnswer(List<StormBidder> bidders) {
        long attempts = 0;
        long sends = 0;
        long answers = 0;
        for (StormBidder bidder : bidders) {
            if (bidder.unanswered > 0) {
                violations.add("bidder %d has %d bids that never got an answer".formatted(bidder.index, bidder.unanswered));
            }
            for (Attempt attempt : bidder.attempts) {
                List<Answer> received = attempt.answersCopy();
                attempts++;
                sends += attempt.sends;
                answers += received.size();
                if (received.isEmpty()) {
                    violations.add("bid %s got no answer".formatted(attempt.clientBidId));
                    continue;
                }
                Set<String> verdicts = received.stream().map(Answer::verdict).collect(Collectors.toSet());
                if (verdicts.size() != 1) {
                    violations.add("bid %s got conflicting answers %s".formatted(attempt.clientBidId, verdicts));
                }
                long computed = received.stream().filter(a -> !a.duplicate()).count();
                if (computed > 1) {
                    violations.add("bid %s was decided %d times".formatted(attempt.clientBidId, computed));
                }
                if (received.size() > attempt.sends) {
                    violations.add("bid %s was sent %d times but answered %d times"
                            .formatted(attempt.clientBidId, attempt.sends, received.size()));
                }
                if (!attempt.disturbed && received.size() != attempt.sends) {
                    violations.add("bid %s was sent %d times on a healthy connection but answered %d times"
                            .formatted(attempt.clientBidId, attempt.sends, received.size()));
                }
                if (received.getFirst().amount() != attempt.amount) {
                    violations.add("bid %s answered for amount %d, sent %d"
                            .formatted(attempt.clientBidId, received.getFirst().amount(), attempt.amount));
                }
            }
        }
        checks.add("every bid attempt got exactly one definitive answer: %d attempts, %d sends, %d answers received"
                .formatted(attempts, sends, answers));
    }

    /** The attempt ledger holds exactly the attempts the clients made, with the same verdicts. */
    private void attemptLedgerMatchesTheClients(List<Long> auctionIds, List<StormBidder> bidders) {
        Map<String, AttemptRow> ledger = new HashMap<>();
        jdbc.sql("""
                        SELECT bidder_id, client_bid_id, auction_id, amount, outcome, reason
                          FROM bid_attempts WHERE auction_id IN (:ids)
                        """)
                .param("ids", auctionIds)
                .query(rs -> {
                    ledger.put(rs.getLong("bidder_id") + ":" + rs.getString("client_bid_id"),
                            new AttemptRow(rs.getLong("auction_id"), rs.getLong("amount"), rs.getString("outcome"),
                                    rs.getString("reason")));
                });
        long clientAttempts = 0;
        for (StormBidder bidder : bidders) {
            for (Attempt attempt : bidder.attempts) {
                clientAttempts++;
                AttemptRow row = ledger.remove(bidder.userId + ":" + attempt.clientBidId);
                List<Answer> received = attempt.answersCopy();
                if (row == null) {
                    violations.add("bid %s is not in the attempt ledger".formatted(attempt.clientBidId));
                } else if (!received.isEmpty()) {
                    Answer answer = received.getFirst();
                    String ledgerReason = row.reason() == null ? "-" : row.reason();
                    if (!row.outcome().equals(answer.outcome()) || !ledgerReason.equals(answer.reason())
                            || row.amount() != attempt.amount || row.auctionId() != bidder.auctionId) {
                        violations.add("bid %s: ledger says %s but the client was told %s"
                                .formatted(attempt.clientBidId, row, answer));
                    }
                }
            }
        }
        if (!ledger.isEmpty()) {
            violations.add("%d ledger attempts were never sent by any client".formatted(ledger.size()));
        }
        checks.add("the attempt ledger contains exactly the %d attempts the clients made".formatted(clientAttempts));
    }

    /** Every ACCEPTED bid is in the bids table and no others are. */
    private void acceptedBidsAreExactlyTheBidsTable(List<StormBidder> bidders, Map<Long, List<BidRow>> bidsByAuction) {
        Map<String, BidRow> stored = new HashMap<>();
        bidsByAuction.values().forEach(rows -> rows.forEach(
                row -> stored.put(row.bidderId() + ":" + row.clientBidId(), row)));
        int totalStored = stored.size();
        long accepted = 0;
        for (StormBidder bidder : bidders) {
            for (Attempt attempt : bidder.attempts) {
                List<Answer> received = attempt.answersCopy();
                boolean toldAccepted = !received.isEmpty() && received.getFirst().outcome().equals("ACCEPTED");
                BidRow row = stored.get(bidder.userId + ":" + attempt.clientBidId);
                if (toldAccepted) {
                    accepted++;
                    stored.remove(bidder.userId + ":" + attempt.clientBidId);
                    if (row == null) {
                        violations.add("accepted bid %s is missing from the bids table".formatted(attempt.clientBidId));
                    } else if (row.amount() != attempt.amount || row.id() != received.getFirst().bidId()
                            || row.auctionId() != bidder.auctionId) {
                        violations.add("accepted bid %s differs from its row %s".formatted(attempt.clientBidId, row));
                    }
                } else if (row != null) {
                    violations.add("bid %s is in the bids table but the client was not told ACCEPTED"
                            .formatted(attempt.clientBidId));
                }
            }
        }
        if (!stored.isEmpty()) {
            violations.add("%d rows in the bids table belong to no accepted client bid".formatted(stored.size()));
        }
        checks.add("the %d bids accepted according to the clients are exactly the %d rows of the bids table"
                .formatted(accepted, totalStored));
    }

    /** Accepted amounts are strictly increasing and each respects the increment at its time. */
    private void pricesFormAValidLadder(Auction auction, List<BidRow> bids) {
        BidRow previous = null;
        for (BidRow bid : bids) {
            long minimum = previous == null ? auction.startPrice() : previous.amount() + auction.minIncrement();
            if (bid.amount() < minimum) {
                violations.add("auction %d: bid %d of %d is below the minimum %d at its time"
                        .formatted(auction.id(), bid.id(), bid.amount(), minimum));
            }
            if (previous != null && bid.amount() <= previous.amount()) {
                violations.add("auction %d: amounts not strictly increasing at bid %d".formatted(auction.id(), bid.id()));
            }
            if (previous != null && previous.bidderId() == bid.bidderId()) {
                violations.add("auction %d: bid %d outbids the bidder's own lead".formatted(auction.id(), bid.id()));
            }
            if (previous != null && bid.seq() <= previous.seq()) {
                violations.add("auction %d: bid sequence numbers out of order at bid %d".formatted(auction.id(), bid.id()));
            }
            previous = bid;
        }
        checks.add("auction %d: %d accepted amounts strictly increasing, each at least previous + %d"
                .formatted(auction.id(), bids.size(), auction.minIncrement()));
    }

    /** The auction's final price and winner equal the last accepted bid, and clients were told so. */
    private void finalStateEqualsTheLastAcceptedBid(Auction auction, List<BidRow> bids, List<StormBidder> subscribers) {
        BidRow last = bids.isEmpty() ? null : bids.getLast();
        Long lastAmount = last == null ? null : last.amount();
        Long lastBidder = last == null ? null : last.bidderId();
        if (!Objects.equals(auction.currentPrice(), lastAmount) || !Objects.equals(auction.leaderId(), lastBidder)) {
            violations.add("auction %d: final price/leader %s/%s but last bid is %s by %s"
                    .formatted(auction.id(), auction.currentPrice(), auction.leaderId(), lastAmount, lastBidder));
        }
        boolean sold = last != null && Auction.reserveMet(lastAmount, auction.reservePrice());
        AuctionStatus expected = sold ? AuctionStatus.SETTLED : AuctionStatus.UNSOLD;
        if (auction.status() != expected) {
            violations.add("auction %d: status %s, expected %s".formatted(auction.id(), auction.status(), expected));
        }
        for (StormBidder subscriber : subscribers) {
            JsonNode closed = subscriber.closedEvent;
            JsonNode settled = subscriber.settledEvent;
            if (closed == null || settled == null) {
                violations.add("bidder %d never received the close/settle events".formatted(subscriber.index));
                continue;
            }
            Long announcedWinner = closed.hasNonNull("winnerId") ? closed.path("winnerId").asLong() : null;
            Long announcedPrice = closed.hasNonNull("finalPrice") ? closed.path("finalPrice").asLong() : null;
            if (!Objects.equals(announcedPrice, lastAmount) || !Objects.equals(announcedWinner, sold ? lastBidder : null)
                    || !settled.path("status").asString().equals(expected.name())) {
                violations.add("bidder %d was told winner %s at %s (%s); the last bid is %s by %s".formatted(
                        subscriber.index, announcedWinner, announcedPrice, settled.path("status").asString(),
                        lastAmount, lastBidder));
            }
        }
        checks.add("auction %d: %s at %s, winner and price equal to the last accepted bid for all %d subscribers"
                .formatted(auction.id(), auction.status(), lastAmount, subscribers.size()));
    }

    /** No accepted bid has a timestamp after the final end time (or before the start). */
    private void noBidIsTimestampedOutsideTheAuction(Auction auction, List<BidRow> bids) {
        for (BidRow bid : bids) {
            if (!bid.acceptedAt().isBefore(auction.endsAt()) || bid.acceptedAt().isBefore(auction.startsAt())) {
                violations.add("auction %d: bid %d accepted at %s, outside [%s, %s)"
                        .formatted(auction.id(), bid.id(), bid.acceptedAt(), auction.startsAt(), auction.endsAt()));
            }
        }
        Instant latest = bids.isEmpty() ? null : bids.getLast().acceptedAt();
        checks.add("auction %d: last bid accepted at %s, %s before the final end time %s".formatted(auction.id(),
                latest, latest == null ? "-" : Duration.between(latest, auction.endsAt()), auction.endsAt()));
    }

    /**
     * The anti-sniping extension count is right: replays the bid timestamps through the rule,
     * independently of the server's audit, and compares with the auction row and the clients.
     */
    private void extensionsMatchTheBidHistory(Auction auction, List<BidRow> bids, List<StormBidder> subscribers) {
        Duration window = Duration.ofSeconds(auction.antiSnipeWindowSeconds());
        Instant deadline = auction.originalEndsAt();
        int extensions = 0;
        for (BidRow bid : bids) {
            boolean inWindow = Duration.between(bid.acceptedAt(), deadline).compareTo(window) <= 0;
            boolean shouldExtend = inWindow && extensions < auction.maxExtensions();
            if (shouldExtend != (bid.extendedTo() != null)) {
                violations.add("auction %d: bid %d %s the deadline but the rule says otherwise"
                        .formatted(auction.id(), bid.id(), bid.extendedTo() != null ? "extended" : "did not extend"));
            }
            if (shouldExtend) {
                deadline = deadline.plus(window);
                extensions++;
            }
        }
        if (extensions != auction.extensionCount() || !deadline.equals(auction.endsAt())) {
            violations.add("auction %d: history implies %d extensions ending %s, row has %d ending %s"
                    .formatted(auction.id(), extensions, deadline, auction.extensionCount(), auction.endsAt()));
        }
        // How many extensions a run reaches depends on how fast the machine answers bids, so
        // it is reported, not demanded per auction. The run as a whole must reach the window
        // (see verify), otherwise it proves nothing about extensions.
        for (StormBidder subscriber : subscribers) {
            if (subscriber.firstSeq == 1 && subscriber.extensionsSeen != auction.extensionCount()) {
                violations.add("bidder %d saw %d TIME_EXTENDED events, the auction has %d"
                        .formatted(subscriber.index, subscriber.extensionsSeen, auction.extensionCount()));
            }
        }
        checks.add("auction %d: %d anti-sniping extensions, re-derived from bid timestamps, end time %s = original + %d x %ds"
                .formatted(auction.id(), extensions, auction.endsAt(), extensions, auction.antiSnipeWindowSeconds()));
    }

    /** Every subscribed client saw the same event sequence with no gaps. */
    private void everySubscriberSawTheSameGapFreeStream(Auction auction, List<StormBidder> subscribers) {
        List<Long> reference = new ArrayList<>();
        jdbc.sql("SELECT seq, payload::text AS payload FROM auction_events WHERE auction_id = :id ORDER BY seq")
                .param("id", auction.id())
                .query(rs -> {
                    if (rs.getLong("seq") != reference.size() + 1) {
                        violations.add("auction %d: event log jumps to seq %d after %d"
                                .formatted(auction.id(), rs.getLong("seq"), reference.size()));
                    }
                    reference.add(StormBidder.fingerprint(JSON.readTree(rs.getString("payload"))));
                });
        if (reference.size() != auction.lastSeq()) {
            violations.add("auction %d: %d events in the log, last_seq %d"
                    .formatted(auction.id(), reference.size(), auction.lastSeq()));
        }
        long delivered = 0;
        for (StormBidder subscriber : subscribers) {
            long[] seen = subscriber.fingerprints();
            delivered += seen.length;
            String who = "bidder %d on auction %d".formatted(subscriber.index, auction.id());
            if (subscriber.gaps > 0) {
                violations.add("%s detected %d gaps in its event stream".formatted(who, subscriber.gaps));
            }
            if (subscriber.midRunSnapshots > 0) {
                violations.add("%s was re-snapshotted %d times instead of replayed".formatted(who, subscriber.midRunSnapshots));
            }
            if (subscriber.lastSeq != auction.lastSeq()) {
                violations.add("%s stopped at seq %d of %d".formatted(who, subscriber.lastSeq, auction.lastSeq()));
            }
            if (subscriber.firstSeq < 1 || seen.length != subscriber.lastSeq - subscriber.firstSeq + 1) {
                violations.add("%s holds %d events for seq %d..%d".formatted(who, seen.length, subscriber.firstSeq,
                        subscriber.lastSeq));
                continue;
            }
            for (int i = 0; i < seen.length; i++) {
                int position = (int) (subscriber.firstSeq - 1 + i);
                if (position >= reference.size() || seen[i] != reference.get(position)) {
                    violations.add("%s received a different event at seq %d than the event log holds"
                            .formatted(who, position + 1));
                    break;
                }
            }
        }
        checks.add("auction %d: all %d subscribers received the identical gap-free stream of %d events (%d deliveries)"
                .formatted(auction.id(), subscribers.size(), reference.size(), delivered));
    }

    private Map<Long, List<BidRow>> loadBids(List<Long> auctionIds) {
        return jdbc.sql("""
                        SELECT id, auction_id, bidder_id, client_bid_id, amount, seq, accepted_at, extended_to
                          FROM bids WHERE auction_id IN (:ids) ORDER BY auction_id, seq
                        """)
                .param("ids", auctionIds)
                .query((rs, row) -> new BidRow(rs.getLong("id"), rs.getLong("auction_id"), rs.getLong("bidder_id"),
                        rs.getString("client_bid_id"), rs.getLong("amount"), rs.getLong("seq"),
                        AuctionRepository.instant(rs, "accepted_at"), AuctionRepository.instant(rs, "extended_to")))
                .list()
                .stream()
                .collect(Collectors.groupingBy(BidRow::auctionId, LinkedHashMap::new, Collectors.toList()));
    }
}
