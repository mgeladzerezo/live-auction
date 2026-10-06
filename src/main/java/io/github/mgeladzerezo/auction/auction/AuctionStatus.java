package io.github.mgeladzerezo.auction.auction;

/**
 * Lifecycle of an auction. Transitions only ever move forward:
 * {@code SCHEDULED -> OPEN -> CLOSED -> SETTLED | UNSOLD}.
 */
public enum AuctionStatus {
    /** Created, start time not reached (or reached but not yet picked up by the scheduler). */
    SCHEDULED,
    /** Accepting bids until the end time on the database clock. */
    OPEN,
    /** Bidding is over; the result has not been audited against the bid history yet. */
    CLOSED,
    /** Audited and sold to the highest bidder at or above the reserve. */
    SETTLED,
    /** Audited and not sold: no bids, or the reserve was not met. */
    UNSOLD;

    /** Whether the auction has passed the point where a bid could ever be accepted again. */
    public boolean isFinished() {
        return this == CLOSED || this == SETTLED || this == UNSOLD;
    }
}
