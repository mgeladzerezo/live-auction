package io.github.mgeladzerezo.auction.metrics;

import io.github.mgeladzerezo.auction.bid.BidResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.function.Supplier;
import org.springframework.stereotype.Component;

/**
 * Every custom meter of the application in one place, so names and tags stay consistent.
 *
 * <ul>
 *   <li>{@code auction.bids{strategy,outcome,reason}}: answered bids</li>
 *   <li>{@code auction.bid.latency{strategy}}: time from command to committed answer</li>
 *   <li>{@code auction.bid.retries{strategy}}: optimistic version conflicts that led to a retry</li>
 *   <li>{@code auction.ws.sessions}, {@code auction.ws.subscriptions}: live gauges</li>
 *   <li>{@code auction.ws.slow.consumers}: sessions dropped because their send queue was full</li>
 *   <li>{@code auction.broadcast.lag}: database commit-time event timestamp to local fan-out</li>
 *   <li>{@code auction.lifecycle.transitions{to}}: auctions opened, closed, settled</li>
 * </ul>
 */
@Component
public class AuctionMetrics {

    private final MeterRegistry registry;
    private final Timer broadcastLag;
    private final Counter slowConsumers;
    private final Counter resyncs;
    private final Counter duplicateBids;

    public AuctionMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.broadcastLag = Timer.builder("auction.broadcast.lag")
                .description("Delay between an event's database timestamp and its fan-out on this instance")
                .publishPercentiles(0.5, 0.99)
                .register(registry);
        this.slowConsumers = Counter.builder("auction.ws.slow.consumers")
                .description("Sessions disconnected because their bounded send queue overflowed")
                .register(registry);
        this.resyncs = Counter.builder("auction.ws.resyncs")
                .description("Subscriptions re-read from the event log after a gap or missed notification")
                .register(registry);
        this.duplicateBids = Counter.builder("auction.bids.duplicates")
                .description("Bid attempts answered from the attempt ledger (same client id seen before)")
                .register(registry);
    }

    /** Records one answered bid and how long the answer took. */
    public void bidAnswered(String strategy, BidResult result, Duration latency) {
        if (result.duplicate()) {
            duplicateBids.increment();
            return;
        }
        String outcome;
        String reason;
        switch (result) {
            case BidResult.Accepted _ -> {
                outcome = "ACCEPTED";
                reason = "NONE";
            }
            case BidResult.Rejected rejected -> {
                outcome = "REJECTED";
                reason = rejected.reason().name();
            }
        }
        registry.counter("auction.bids", "strategy", strategy, "outcome", outcome, "reason", reason).increment();
        Timer.builder("auction.bid.latency")
                .tag("strategy", strategy)
                .publishPercentiles(0.5, 0.99)
                .register(registry)
                .record(latency);
    }

    /** An optimistic attempt lost the version race and will be retried or given up. */
    public void bidRetried(String strategy) {
        registry.counter("auction.bid.retries", "strategy", strategy).increment();
    }

    public void broadcastLag(Duration lag) {
        broadcastLag.record(lag.isNegative() ? Duration.ZERO : lag);
    }

    public void slowConsumerDropped() {
        slowConsumers.increment();
    }

    public void subscriptionResynced() {
        resyncs.increment();
    }

    public void lifecycleTransition(String to, int count) {
        if (count > 0) {
            registry.counter("auction.lifecycle.transitions", "to", to).increment(count);
        }
    }

    public void settlementBlocked() {
        registry.counter("auction.lifecycle.settlement.blocked").increment();
    }

    public void gauge(String name, String description, Supplier<Number> value) {
        Gauge.builder(name, value).description(description).register(registry);
    }
}
