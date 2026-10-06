package io.github.mgeladzerezo.auction.auction;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mgeladzerezo.auction.auth.User;
import io.github.mgeladzerezo.auction.bid.BidService;
import io.github.mgeladzerezo.auction.support.AbstractIntegrationTest;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Status transitions driven by the database clock, and the audit that guards settlement.
 */
class LifecycleIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    BidService bids;
    @Autowired
    ConsistencyAudit audit;

    private List<String> eventTypes(long auctionId) {
        return jdbc.sql("SELECT type FROM auction_events WHERE auction_id = :id ORDER BY seq")
                .param("id", auctionId).query(String.class).list();
    }

    @Test
    void auctionOpensOnlyOnceItsStartTimeHasComeOnTheDatabaseClock() throws Exception {
        long id = auctions.insert(auction().startsIn(Duration.ofMillis(700)).duration(Duration.ofMinutes(5)).build());
        Auction scheduled = reload(id);
        assertThat(scheduled.status()).isEqualTo(AuctionStatus.SCHEDULED);
        assertThat(scheduled.lastSeq()).isZero();
        assertThat(Duration.between(scheduled.startsAt(), scheduled.endsAt())).isEqualTo(Duration.ofMinutes(5));

        assertThat(lifecycle.openDue()).isZero();
        assertThat(reload(id).status()).isEqualTo(AuctionStatus.SCHEDULED);

        awaitDbTime(scheduled.startsAt());
        assertThat(lifecycle.openDue()).isEqualTo(1);
        assertThat(lifecycle.openDue()).as("already open").isZero();
        Auction opened = reload(id);
        assertThat(opened.status()).isEqualTo(AuctionStatus.OPEN);
        assertThat(opened.version()).isEqualTo(scheduled.version() + 1);
        assertThat(eventTypes(id)).containsExactly("AUCTION_OPENED");
    }

    @Test
    void auctionWithBidsAboveTheReserveIsClosedThenSettledToTheLeader() throws Exception {
        Auction auction = open(auction().duration(Duration.ofMillis(1200)).reservePrice(1500L));
        User alice = newUser("alice");
        User bob = newUser("bob");
        bids.place(bid(auction, alice, 1000));
        bids.place(bid(auction, bob, 1600));

        assertThat(lifecycle.closeDue()).as("not due yet").isZero();
        awaitDbTime(auction.endsAt());
        assertThat(lifecycle.closeDue()).isEqualTo(1);

        Auction closed = reload(auction.id());
        assertThat(closed.status()).isEqualTo(AuctionStatus.CLOSED);
        assertThat(closed.closedAt()).isAfterOrEqualTo(auction.endsAt());

        assertThat(lifecycle.settleClosed()).isEqualTo(1);
        Auction settled = reload(auction.id());
        assertThat(settled.status()).isEqualTo(AuctionStatus.SETTLED);
        assertThat(settled.leaderId()).isEqualTo(bob.id());
        assertThat(settled.currentPrice()).isEqualTo(1600);
        assertThat(eventTypes(auction.id())).containsExactly(
                "AUCTION_OPENED", "BID_ACCEPTED", "BID_ACCEPTED", "AUCTION_CLOSED", "AUCTION_SETTLED");
        assertThat(audit.check(auction.id()).consistent()).isTrue();
    }

    @Test
    void auctionWithoutBidsOrBelowReserveEndsUnsoldWithNoWinner() throws Exception {
        Auction noBids = open(auction().duration(Duration.ofMillis(900)));
        Auction belowReserve = open(auction().duration(Duration.ofMillis(900)).reservePrice(50_000L));
        bids.place(bid(belowReserve, newUser("alice"), 1000));

        awaitDbTime(belowReserve.endsAt());
        lifecycle.tick();

        assertThat(reload(noBids.id()).status()).isEqualTo(AuctionStatus.UNSOLD);
        assertThat(reload(belowReserve.id()).status()).isEqualTo(AuctionStatus.UNSOLD);
        String closedEvent = jdbc.sql(
                        "SELECT payload::text FROM auction_events WHERE auction_id = :id AND type = 'AUCTION_CLOSED'")
                .param("id", belowReserve.id()).query(String.class).single();
        assertThat(closedEvent).contains("\"reserveMet\": false").contains("\"finalPrice\": 1000")
                .doesNotContain("winnerId");
    }

    @Test
    void auditPassesForAHealthyAuctionAndPinpointsEveryKindOfDrift() {
        Auction auction = open(auction().duration(Duration.ofSeconds(20)).antiSniping(30, 3));
        User alice = newUser("alice");
        User bob = newUser("bob");
        bids.place(bid(auction, alice, 1000));
        bids.place(bid(auction, bob, 1100));
        assertThat(audit.check(auction.id()).violations()).isEmpty();
        assertThat(audit.check(auction.id()).bids()).isEqualTo(2);

        assertDrift(auction.id(), "current_price = 9999", "current_price is 9999 but the last bid is 1100");
        assertDrift(auction.id(), "leader_id = " + alice.id(), "leader_id is");
        assertDrift(auction.id(), "bid_count = 5", "bid_count is 5 but there are 2 bids");
        assertDrift(auction.id(), "ends_at = ends_at + interval '30 seconds'", "end time is");
        assertDrift(auction.id(), "extension_count = 2", "extension_count is 2 but the bid history implies 1");
        assertDrift(auction.id(), "last_seq = last_seq + 1", "event log has");
        assertDrift(auction.id(), "min_increment = 500", "is below the minimum");
    }

    @Test
    void settlementRefusesAnAuctionWhoseHeadDisagreesWithItsBidHistory() throws Exception {
        Auction auction = open(auction().duration(Duration.ofMillis(1200)));
        User alice = newUser("alice");
        bids.place(bid(auction, alice, 1000));
        awaitDbTime(auction.endsAt());
        lifecycle.closeDue();

        // Simulate the bug the audit exists for: the denormalised price drifted from the history.
        jdbc.sql("UPDATE auctions SET current_price = 777777 WHERE id = :id").param("id", auction.id()).update();

        assertThat(lifecycle.settleClosed()).isZero();
        assertThat(reload(auction.id()).status()).as("never settled on a wrong price").isEqualTo(AuctionStatus.CLOSED);
        String error = jdbc.sql("SELECT settle_error FROM auctions WHERE id = :id")
                .param("id", auction.id()).query(String.class).single();
        assertThat(error).contains("current_price is 777777 but the last bid is 1000");
        assertThat(lifecycle.settleClosed()).as("a blocked auction is not retried in a loop").isZero();
        assertThat(eventTypes(auction.id())).doesNotContain("AUCTION_SETTLED");
    }

    /** Applies one corruption to the auction row, checks the audit names it, and undoes it. */
    private void assertDrift(long auctionId, String corruption, String expectedViolation) {
        Auction before = reload(auctionId);
        jdbc.sql("UPDATE auctions SET " + corruption + " WHERE id = :id").param("id", auctionId).update();
        try {
            assertThat(audit.check(auctionId).violations()).as(corruption)
                    .anySatisfy(violation -> assertThat(violation).contains(expectedViolation));
        } finally {
            jdbc.sql("""
                            UPDATE auctions SET current_price = :price, leader_id = :leader, bid_count = :bids,
                                   ends_at = :endsAt, extension_count = :extensions, last_seq = :lastSeq,
                                   min_increment = :increment
                             WHERE id = :id
                            """)
                    .param("price", before.currentPrice())
                    .param("leader", before.leaderId())
                    .param("bids", before.bidCount())
                    .param("endsAt", before.endsAt().atOffset(java.time.ZoneOffset.UTC))
                    .param("extensions", before.extensionCount())
                    .param("lastSeq", before.lastSeq())
                    .param("increment", before.minIncrement())
                    .param("id", auctionId)
                    .update();
        }
        assertThat(audit.check(auctionId).violations()).as("restored after " + corruption).isEmpty();
    }
}
