package io.github.mgeladzerezo.auction.bid;

/**
 * How concurrent bids on one auction are kept from overwriting each other. Implementations
 * share the rules and the SQL in {@link BidLedger}; they differ only in locking.
 */
public interface BidStrategy {

    /** Name used in configuration ({@code auction.bidding.strategy}) and as a metric tag. */
    String name();

    /**
     * Places the bid and returns its one definitive answer, which is committed by the time
     * this method returns.
     */
    BidResult place(BidCommand command);
}
