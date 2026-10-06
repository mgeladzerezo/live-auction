package io.github.mgeladzerezo.auction.bid;

/** Why a bid was definitively rejected. Sent to the client as the reason code. */
public enum RejectReason {
    /** Below the start price, or below current price plus the minimum increment. */
    TOO_LOW,
    /** The end time has passed on the database clock, or the auction was already closed. */
    AUCTION_CLOSED,
    /** The auction has not been opened yet. */
    NOT_STARTED,
    /** The bidder already holds the leading bid. */
    ALREADY_LEADING,
    /** Sellers may not bid on their own auction. */
    SELLER_CANNOT_BID,
    AUCTION_NOT_FOUND,
    /**
     * The optimistic strategy lost the version race on every allowed attempt. The bid might
     * have been valid; the client may bid again with a new id.
     */
    CONTENTION
}
