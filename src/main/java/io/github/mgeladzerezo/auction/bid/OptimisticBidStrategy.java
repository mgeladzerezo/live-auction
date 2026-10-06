package io.github.mgeladzerezo.auction.bid;

import io.github.mgeladzerezo.auction.bid.BidLedger.Attempt;
import io.github.mgeladzerezo.auction.bid.BidLedger.LockMode;
import io.github.mgeladzerezo.auction.config.AuctionProperties;
import io.github.mgeladzerezo.auction.metrics.AuctionMetrics;
import java.time.Duration;
import org.springframework.stereotype.Component;

/**
 * Optimistic locking: read without a lock, write with {@code WHERE version = ?}, and if the
 * row moved underneath, back off and start over with a fresh read.
 *
 * <p>Each attempt is its own short transaction, so no lock and no snapshot is held while
 * sleeping. A retry re-evaluates the rules against the new state; in an auction that usually
 * ends the loop early, because the bid that won the race raised the price and the loser is
 * now simply {@code TOO_LOW}. Retries are bounded: after {@code maxAttempts} conflicts the bid
 * is definitively rejected with {@code CONTENTION} rather than left spinning.
 */
@Component
public class OptimisticBidStrategy implements BidStrategy {

    public static final String NAME = "optimistic";

    private final BidLedger ledger;
    private final AuctionMetrics metrics;
    private final int maxAttempts;
    private final Backoff backoff;

    public OptimisticBidStrategy(BidLedger ledger, AuctionMetrics metrics, AuctionProperties properties) {
        this.ledger = ledger;
        this.metrics = metrics;
        this.maxAttempts = Math.max(1, properties.bidding().maxAttempts());
        this.backoff = new Backoff(properties.bidding().backoffBase(), properties.bidding().backoffCap());
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public BidResult place(BidCommand command) {
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            if (ledger.attempt(command, LockMode.NONE) instanceof Attempt.Done(BidResult result)) {
                return result;
            }
            metrics.bidRetried(NAME);
            if (attempt < maxAttempts) {
                sleep(backoff.next(attempt));
            }
        }
        return ledger.rejectAfterContention(command);
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while backing off", e);
        }
    }
}
