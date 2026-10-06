package io.github.mgeladzerezo.auction.bid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.mgeladzerezo.auction.auction.Auction;
import io.github.mgeladzerezo.auction.auction.ConsistencyAudit;
import io.github.mgeladzerezo.auction.auth.User;
import io.github.mgeladzerezo.auction.support.AbstractIntegrationTest;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;

/**
 * The bidding rules end to end against PostgreSQL, once per locking strategy: every reason
 * code, idempotent retries, anti-sniping arithmetic, and the append-only guarantees.
 */
class BiddingIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    List<BidStrategy> strategies;
    @Autowired
    ConsistencyAudit audit;

    private BidStrategy strategy(String name) {
        return strategies.stream().filter(s -> s.name().equals(name)).findFirst().orElseThrow();
    }

    @ParameterizedTest
    @ValueSource(strings = {"optimistic", "pessimistic"})
    void firstBidAtStartPriceIsAcceptedAndBecomesTheHead(String name) {
        Auction auction = open(auction().startPrice(5000).minIncrement(250));
        User alice = newUser("alice");

        BidResult result = strategy(name).place(bid(auction, alice, 5000));

        assertThat(result).isInstanceOfSatisfying(BidResult.Accepted.class, accepted -> {
            assertThat(accepted.amount()).isEqualTo(5000);
            assertThat(accepted.seq()).isEqualTo(2); // seq 1 was AUCTION_OPENED
            assertThat(accepted.extendedTo()).isNull();
            assertThat(accepted.duplicate()).isFalse();
        });
        Auction after = reload(auction.id());
        assertThat(after.currentPrice()).isEqualTo(5000);
        assertThat(after.leaderId()).isEqualTo(alice.id());
        assertThat(after.bidCount()).isEqualTo(1);
        assertThat(after.version()).isEqualTo(auction.version() + 1);
        assertThat(after.minimumNextBid()).isEqualTo(5250);
    }

    @ParameterizedTest
    @ValueSource(strings = {"optimistic", "pessimistic"})
    void rejectsWithTheRightReasonCode(String name) {
        BidStrategy strategy = strategy(name);
        User seller = newUser("seller");
        User alice = newUser("alice");
        User bob = newUser("bob");
        Auction auction = open(auction().startPrice(1000).minIncrement(100).seller(seller.id()));

        assertRejected(strategy.place(bid(auction, alice, 999)), RejectReason.TOO_LOW);
        assertRejected(strategy.place(bid(auction, seller, 1000)), RejectReason.SELLER_CANNOT_BID);
        assertThat(strategy.place(bid(auction, alice, 1000))).isInstanceOf(BidResult.Accepted.class);
        assertRejected(strategy.place(bid(auction, alice, 5000)), RejectReason.ALREADY_LEADING);
        assertRejected(strategy.place(bid(auction, bob, 1099)), RejectReason.TOO_LOW);
        assertThat(strategy.place(bid(auction, bob, 1100))).isInstanceOf(BidResult.Accepted.class);

        BidResult.Rejected tooLow = (BidResult.Rejected) strategy.place(bid(auction, alice, 1150));
        assertThat(tooLow.currentPrice()).isEqualTo(1100);
        assertThat(tooLow.minimumBid()).isEqualTo(1200);

        assertRejected(strategy.place(new BidCommand(999_999_999L, alice.id(), alice.username(), 1000, "missing-1")),
                RejectReason.AUCTION_NOT_FOUND);

        long scheduledId = auctions.insert(auction().startsIn(Duration.ofHours(1)).build());
        assertRejected(strategy.place(bid(reload(scheduledId), alice, 1000)), RejectReason.NOT_STARTED);

        assertThat(count("bids", auction.id())).isEqualTo(2);
        assertThat(audit.check(auction.id()).violations()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"optimistic", "pessimistic"})
    void deadlineOnTheDatabaseClockEndsBiddingEvenBeforeTheSchedulerCloses(String name) throws Exception {
        Auction auction = open(auction().duration(Duration.ofMillis(300)));
        User alice = newUser("alice");

        awaitDbTime(auction.endsAt());

        // No scheduler has run: the row still says OPEN. The deadline alone must stop the bid.
        assertThat(reload(auction.id()).status().name()).isEqualTo("OPEN");
        assertRejected(strategy(name).place(bid(auction, alice, 1000)), RejectReason.AUCTION_CLOSED);

        lifecycle.tick();
        assertRejected(strategy(name).place(bid(auction, alice, 1000)), RejectReason.AUCTION_CLOSED);
        assertThat(count("bids", auction.id())).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = {"optimistic", "pessimistic"})
    void resendingTheSameClientIdReturnsTheOriginalAnswerAndNeverASecondBid(String name) {
        BidStrategy strategy = strategy(name);
        Auction auction = open(auction());
        User alice = newUser("alice");
        User bob = newUser("bob");

        BidCommand accepted = bid(auction, alice, 1000);
        BidResult.Accepted first = (BidResult.Accepted) strategy.place(accepted);
        BidResult.Accepted again = (BidResult.Accepted) strategy.place(accepted);
        assertThat(again.duplicate()).isTrue();
        assertThat(again.bidId()).isEqualTo(first.bidId());
        assertThat(again.seq()).isEqualTo(first.seq());
        assertThat(again.acceptedAt()).isEqualTo(first.acceptedAt());

        // A rejected attempt stays rejected even when the same command would now succeed.
        BidCommand low = bid(auction, bob, 1050);
        assertRejected(strategy.place(low), RejectReason.TOO_LOW);
        BidCommand wouldNowWin = new BidCommand(auction.id(), bob.id(), bob.username(), 5000, low.clientBidId());
        BidResult.Rejected replayed = (BidResult.Rejected) strategy.place(wouldNowWin);
        assertThat(replayed.duplicate()).isTrue();
        assertThat(replayed.reason()).isEqualTo(RejectReason.TOO_LOW);
        assertThat(replayed.amount()).isEqualTo(1050);

        // The same id from a different bidder is a different attempt.
        BidCommand sameIdOtherBidder = new BidCommand(auction.id(), bob.id(), bob.username(), 1100, accepted.clientBidId());
        assertThat(strategy.place(sameIdOtherBidder)).isInstanceOf(BidResult.Accepted.class);

        Auction after = reload(auction.id());
        assertThat(after.bidCount()).isEqualTo(2);
        assertThat(after.currentPrice()).isEqualTo(1100);
        assertThat(count("bids", auction.id())).isEqualTo(2);
        assertThat(count("bid_attempts", auction.id())).isEqualTo(3);
        assertThat(audit.check(auction.id()).violations()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"optimistic", "pessimistic"})
    void bidInsideTheWindowExtendsTheEndTimeInTheSameTransaction(String name) {
        BidStrategy strategy = strategy(name);
        // The whole auction is shorter than the window, so every bid is "in the last N seconds".
        Auction auction = open(auction().duration(Duration.ofSeconds(20)).antiSniping(30, 2));
        User alice = newUser("alice");
        User bob = newUser("bob");

        BidResult.Accepted first = (BidResult.Accepted) strategy.place(bid(auction, alice, 1000));
        assertThat(first.extendedTo()).isEqualTo(auction.endsAt().plusSeconds(30));
        Auction afterFirst = reload(auction.id());
        assertThat(afterFirst.endsAt()).isEqualTo(auction.endsAt().plusSeconds(30));
        assertThat(afterFirst.extensionCount()).isEqualTo(1);
        // BID_ACCEPTED and TIME_EXTENDED took two consecutive sequence numbers.
        assertThat(afterFirst.lastSeq()).isEqualTo(auction.lastSeq() + 2);

        // 50 s left now, outside the 30 s window: accepted without extending.
        BidResult.Accepted second = (BidResult.Accepted) strategy.place(bid(auction, bob, 1100));
        assertThat(second.extendedTo()).isNull();
        assertThat(reload(auction.id()).endsAt()).isEqualTo(afterFirst.endsAt());
        assertThat(reload(auction.id()).extensionCount()).isEqualTo(1);
        assertThat(audit.check(auction.id()).violations()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"optimistic", "pessimistic"})
    void extensionsStopAtTheConfiguredMaximum(String name) throws Exception {
        BidStrategy strategy = strategy(name);
        Auction auction = open(auction().duration(Duration.ofMillis(800)).antiSniping(1, 2));
        Instant originalEnd = auction.originalEndsAt();
        User alice = newUser("alice");
        User bob = newUser("bob");

        // Under 1 s left: first extension.
        BidResult.Accepted first = (BidResult.Accepted) strategy.place(bid(auction, alice, 1000));
        assertThat(first.extendedTo()).isEqualTo(originalEnd.plusSeconds(1));

        // Past the original end, inside the extended one, again under 1 s left: second extension.
        awaitDbTime(originalEnd.plusMillis(100));
        BidResult.Accepted second = (BidResult.Accepted) strategy.place(bid(auction, bob, 1100));
        assertThat(second.extendedTo()).isEqualTo(originalEnd.plusSeconds(2));

        // Under 1 s left once more, but both extensions are used up: accepted, end time stays.
        awaitDbTime(originalEnd.plusMillis(1100));
        BidResult.Accepted third = (BidResult.Accepted) strategy.place(bid(auction, alice, 1200));
        assertThat(third.extendedTo()).isNull();

        Auction after = reload(auction.id());
        assertThat(after.extensionCount()).isEqualTo(2);
        assertThat(after.endsAt()).isEqualTo(originalEnd.plusSeconds(2));
        assertThat(count("auction_events", auction.id())).isEqualTo(1 + 3 + 2);
        assertThat(audit.check(auction.id()).violations()).isEmpty();

        // And once that final end time passes, the auction is over.
        awaitDbTime(after.endsAt());
        assertRejected(strategy.place(bid(auction, bob, 1300)), RejectReason.AUCTION_CLOSED);
    }

    @Test
    void acceptedBidTimestampIsTheDatabaseClockReadingOfTheDecision() {
        Auction auction = open(auction());
        Instant before = dbNow();
        BidResult.Accepted accepted = (BidResult.Accepted) strategy("optimistic").place(bid(auction, newUser("alice"), 1000));
        Instant after = dbNow();

        assertThat(accepted.acceptedAt()).isBetween(before, after);
        Instant stored = jdbc.sql("SELECT accepted_at FROM bids WHERE id = :id").param("id", accepted.bidId())
                .query(java.time.OffsetDateTime.class).single().toInstant();
        assertThat(stored).isEqualTo(accepted.acceptedAt());
    }

    @Test
    void historyTablesRefuseUpdatesAndDeletes() {
        Auction auction = open(auction());
        strategy("optimistic").place(bid(auction, newUser("alice"), 1000));

        for (String statement : List.of(
                "UPDATE bids SET amount = amount + 1 WHERE auction_id = :id",
                "DELETE FROM bids WHERE auction_id = :id",
                "UPDATE bid_attempts SET outcome = 'REJECTED', reason = 'TOO_LOW' WHERE auction_id = :id",
                "DELETE FROM auction_events WHERE auction_id = :id")) {
            assertThatThrownBy(() -> jdbc.sql(statement).param("id", auction.id()).update())
                    .as(statement)
                    .isInstanceOf(DataAccessException.class)
                    .hasMessageContaining("append-only");
        }
    }

    @Test
    void commandValidationRejectsBadAmountsAndIds() {
        assertThatThrownBy(() -> new BidCommand(1, 1, "a", 0, "ok")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BidCommand(1, 1, "a", BidCommand.MAX_AMOUNT + 1, "ok"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BidCommand(1, 1, "a", 100, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BidCommand(1, 1, "a", 100, "has space")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BidCommand(1, 1, "a", 100, "x".repeat(65))).isInstanceOf(IllegalArgumentException.class);
    }

    private static void assertRejected(BidResult result, RejectReason reason) {
        assertThat(result).isInstanceOfSatisfying(BidResult.Rejected.class,
                rejected -> assertThat(rejected.reason()).isEqualTo(reason));
    }
}
