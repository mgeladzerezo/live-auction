package io.github.mgeladzerezo.auction.bid;

import io.github.mgeladzerezo.auction.config.AuctionProperties;
import io.github.mgeladzerezo.auction.metrics.AuctionMetrics;
import java.time.Duration;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * Entry point for placing bids, used by the WebSocket handler, the REST endpoint and the demo
 * bots alike. Picks the configured {@link BidStrategy} and records metrics.
 */
@Service
public class BidService {

    private final BidStrategy strategy;
    private final AuctionMetrics metrics;

    public BidService(List<BidStrategy> strategies, AuctionProperties properties, AuctionMetrics metrics) {
        String wanted = properties.bidding().strategy();
        this.strategy = strategies.stream()
                .filter(candidate -> candidate.name().equalsIgnoreCase(wanted))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Unknown auction.bidding.strategy '" + wanted
                        + "'; available: " + strategies.stream().map(BidStrategy::name).toList()));
        this.metrics = metrics;
    }

    /**
     * Places a bid and returns its definitive, already committed answer.
     *
     * @throws RuntimeException on an infrastructure failure (for example no database
     *                          connection). The outcome is then unknown to the caller, who
     *                          should retry with the same {@code clientBidId}; idempotency
     *                          turns the retry into the definitive answer.
     */
    public BidResult place(BidCommand command) {
        long started = System.nanoTime();
        BidResult result = strategy.place(command);
        metrics.bidAnswered(strategy.name(), result, Duration.ofNanos(System.nanoTime() - started));
        return result;
    }

    /** Name of the locking strategy in use. */
    public String strategyName() {
        return strategy.name();
    }
}
