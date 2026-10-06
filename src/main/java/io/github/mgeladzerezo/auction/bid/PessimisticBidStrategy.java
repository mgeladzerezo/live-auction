package io.github.mgeladzerezo.auction.bid;

import io.github.mgeladzerezo.auction.bid.BidLedger.Attempt;
import io.github.mgeladzerezo.auction.bid.BidLedger.LockMode;
import org.springframework.stereotype.Component;

/**
 * Pessimistic locking: {@code SELECT ... FOR UPDATE} the auction row, decide, write, commit.
 *
 * <p>Bids on one auction queue on the row lock and are processed strictly one at a time, so
 * there is never a conflict and never a retry. The price is that every bid, including the many
 * that are about to be rejected as too low, waits in that queue while holding a pooled
 * connection, and a hot auction can drain the pool for everyone else.
 */
@Component
public class PessimisticBidStrategy implements BidStrategy {

    public static final String NAME = "pessimistic";

    private final BidLedger ledger;

    public PessimisticBidStrategy(BidLedger ledger) {
        this.ledger = ledger;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public BidResult place(BidCommand command) {
        if (ledger.attempt(command, LockMode.FOR_UPDATE) instanceof Attempt.Done(BidResult result)) {
            return result;
        }
        throw new IllegalStateException("Auction row changed while it was locked FOR UPDATE");
    }
}
