package io.github.mgeladzerezo.auction.bid;

import io.github.mgeladzerezo.auction.auction.AuctionStatus;
import java.time.Duration;
import java.time.Instant;

/**
 * The bidding rules as a pure function of an auction snapshot and a bid.
 *
 * <p>Nothing here touches the database or a clock: the snapshot brings its own
 * {@link BidState#dbNow() database time}. Both locking strategies call this function; they
 * differ only in how they guarantee that the snapshot is still current when the decision is
 * written.
 */
public final class BidRules {

    private BidRules() {
    }

    /** Outcome of applying the rules to one snapshot. */
    public sealed interface Decision {

        /**
         * @param endsAt   end time after this bid: unchanged, or pushed out by the anti-sniping window
         * @param extended whether this bid triggered an extension
         */
        record Accept(Instant endsAt, boolean extended) implements Decision {
        }

        record Reject(RejectReason reason) implements Decision {
        }
    }

    /**
     * Decides a bid. Checks run in a fixed order so that the reason code is deterministic:
     * auction state first, then who is bidding, then the amount.
     */
    public static Decision decide(BidState state, long bidderId, long amount) {
        if (state.status() == AuctionStatus.SCHEDULED) {
            return new Decision.Reject(RejectReason.NOT_STARTED);
        }
        // An OPEN row whose end time has passed is closed for bidding even if the scheduler has
        // not flipped its status yet: the deadline, not the status flag, ends the auction.
        if (state.status() != AuctionStatus.OPEN || !state.dbNow().isBefore(state.endsAt())) {
            return new Decision.Reject(RejectReason.AUCTION_CLOSED);
        }
        if (state.sellerId() != null && state.sellerId() == bidderId) {
            return new Decision.Reject(RejectReason.SELLER_CANNOT_BID);
        }
        if (state.leaderId() != null && state.leaderId() == bidderId) {
            return new Decision.Reject(RejectReason.ALREADY_LEADING);
        }
        if (amount < state.minimumBid()) {
            return new Decision.Reject(RejectReason.TOO_LOW);
        }
        return extendsDeadline(state)
                ? new Decision.Accept(state.endsAt().plusSeconds(state.antiSnipeWindowSeconds()), true)
                : new Decision.Accept(state.endsAt(), false);
    }

    /**
     * Anti-sniping: a bid accepted with no more than the window left on the clock adds one
     * window to the end time, until the auction has used up its extensions.
     */
    private static boolean extendsDeadline(BidState state) {
        if (state.antiSnipeWindowSeconds() <= 0 || state.extensionCount() >= state.maxExtensions()) {
            return false;
        }
        Duration remaining = Duration.between(state.dbNow(), state.endsAt());
        return remaining.compareTo(Duration.ofSeconds(state.antiSnipeWindowSeconds())) <= 0;
    }
}
