package io.github.mgeladzerezo.auction.bid;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mgeladzerezo.auction.auction.AuctionStatus;
import io.github.mgeladzerezo.auction.bid.BidRules.Decision;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

/** The bid rules as a pure function: no database, no clock, every branch. */
class BidRulesTest {

    private static final Instant NOW = Instant.parse("2026-01-01T12:00:00Z");
    private static final long SELLER = 1;
    private static final long ALICE = 2;
    private static final long BOB = 3;

    /** An open auction, 10 minutes left, start 1000, increment 100, 30 s window, 3 extensions. */
    private static final BidState FRESH = new BidState(7, AuctionStatus.OPEN, SELLER, 1000, 100, null, null,
            NOW.plusSeconds(600), 30, 3, 0, 0, 5, 1, NOW);

    private static BidState led(long price, long leader) {
        return new BidState(7, AuctionStatus.OPEN, SELLER, 1000, 100, price, leader,
                FRESH.endsAt(), 30, 3, 0, 1, 6, 2, NOW);
    }

    private static BidState endingIn(long millis, int extensionsUsed) {
        return new BidState(7, AuctionStatus.OPEN, SELLER, 1000, 100, null, null,
                NOW.plusMillis(millis), 30, 3, extensionsUsed, 0, 5, 1, NOW);
    }

    private static BidState withStatus(AuctionStatus status) {
        return new BidState(7, status, SELLER, 1000, 100, null, null, FRESH.endsAt(), 30, 3, 0, 0, 5, 1, NOW);
    }

    private static void assertRejected(Decision decision, RejectReason reason) {
        assertThat(decision).isEqualTo(new Decision.Reject(reason));
    }

    @Test
    void openingBidMustReachTheStartPrice() {
        assertRejected(BidRules.decide(FRESH, ALICE, 999), RejectReason.TOO_LOW);
        assertThat(BidRules.decide(FRESH, ALICE, 1000)).isEqualTo(new Decision.Accept(FRESH.endsAt(), false));
    }

    @ParameterizedTest
    @CsvSource({"2000, 2099, false", "2000, 2100, true", "2000, 2101, true", "2000, 2000, false", "2000, 1, false"})
    void laterBidsMustBeatThePriceByTheIncrement(long price, long amount, boolean accepted) {
        Decision decision = BidRules.decide(led(price, ALICE), BOB, amount);
        if (accepted) {
            assertThat(decision).isInstanceOf(Decision.Accept.class);
        } else {
            assertRejected(decision, RejectReason.TOO_LOW);
        }
    }

    @Test
    void leaderCannotOutbidThemselves() {
        assertRejected(BidRules.decide(led(2000, ALICE), ALICE, 9000), RejectReason.ALREADY_LEADING);
    }

    @Test
    void sellerCannotBid() {
        assertRejected(BidRules.decide(FRESH, SELLER, 5000), RejectReason.SELLER_CANNOT_BID);
    }

    @Test
    void scheduledAuctionIsNotStarted() {
        assertRejected(BidRules.decide(withStatus(AuctionStatus.SCHEDULED), ALICE, 5000), RejectReason.NOT_STARTED);
    }

    @ParameterizedTest
    @EnumSource(value = AuctionStatus.class, names = {"CLOSED", "SETTLED", "UNSOLD"})
    void finishedAuctionIsClosed(AuctionStatus status) {
        assertRejected(BidRules.decide(withStatus(status), ALICE, 5000), RejectReason.AUCTION_CLOSED);
    }

    @Test
    void deadlineIsExclusiveAndDoesNotNeedTheStatusToHaveChanged() {
        // Status still OPEN: only the clock says it is over.
        assertRejected(BidRules.decide(endingIn(0, 0), ALICE, 5000), RejectReason.AUCTION_CLOSED);
        assertRejected(BidRules.decide(endingIn(-1, 0), ALICE, 5000), RejectReason.AUCTION_CLOSED);
        assertThat(BidRules.decide(endingIn(1, 3), ALICE, 5000)).isInstanceOf(Decision.Accept.class);
    }

    @Test
    void stateIsCheckedBeforeBidderAndBidderBeforeAmount() {
        // A too-low bid by the seller on a closed auction reports the auction, not the bidder or amount.
        assertRejected(BidRules.decide(withStatus(AuctionStatus.CLOSED), SELLER, 1), RejectReason.AUCTION_CLOSED);
        assertRejected(BidRules.decide(FRESH, SELLER, 1), RejectReason.SELLER_CANNOT_BID);
        assertRejected(BidRules.decide(led(2000, ALICE), ALICE, 1), RejectReason.ALREADY_LEADING);
    }

    @ParameterizedTest
    @CsvSource({
            // millis left, extensions used, extends?
            "30001, 0, false",   // just outside the 30 s window
            "30000, 0, true",    // exactly on the boundary counts as inside
            "29999, 0, true",
            "1,     0, true",
            "1,     2, true",    // last extension still available
            "1,     3, false",   // all three used
            "600000, 0, false"
    })
    void antiSnipingExtendsOnlyInsideTheWindowAndOnlyWhileExtensionsRemain(long millisLeft, int used, boolean extended) {
        BidState state = endingIn(millisLeft, used);

        Decision.Accept accept = (Decision.Accept) BidRules.decide(state, ALICE, 1000);

        assertThat(accept.extended()).isEqualTo(extended);
        assertThat(accept.endsAt()).isEqualTo(extended ? state.endsAt().plusSeconds(30) : state.endsAt());
    }

    @Test
    void windowOfZeroDisablesAntiSniping() {
        BidState noWindow = new BidState(7, AuctionStatus.OPEN, SELLER, 1000, 100, null, null,
                NOW.plusMillis(5), 0, 3, 0, 0, 5, 1, NOW);
        assertThat(BidRules.decide(noWindow, ALICE, 1000)).isEqualTo(new Decision.Accept(noWindow.endsAt(), false));
    }

    @Test
    void rejectedBidNeverExtends() {
        assertRejected(BidRules.decide(endingIn(5, 0), ALICE, 1), RejectReason.TOO_LOW);
    }
}
