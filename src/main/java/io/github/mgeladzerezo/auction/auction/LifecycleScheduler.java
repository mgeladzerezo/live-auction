package io.github.mgeladzerezo.auction.auction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives {@link AuctionLifecycle} on a fixed delay. Every instance may run it; the lifecycle
 * claims rows with {@code SKIP LOCKED}, so instances share the work instead of repeating it.
 * Can be switched off ({@code auction.lifecycle.enabled=false}) for tests that need to decide
 * themselves when a close happens.
 */
@Component
@ConditionalOnProperty(name = "auction.lifecycle.enabled", havingValue = "true", matchIfMissing = true)
public class LifecycleScheduler {

    private static final Logger log = LoggerFactory.getLogger(LifecycleScheduler.class);

    private final AuctionLifecycle lifecycle;

    public LifecycleScheduler(AuctionLifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    @Scheduled(fixedDelayString = "${auction.lifecycle.tick:250ms}")
    void tick() {
        try {
            lifecycle.tick();
        } catch (RuntimeException e) {
            // Nothing is lost: whatever was due is still due on the next pass.
            log.warn("Lifecycle pass failed, will retry on the next tick: {}", e.toString());
        }
    }
}
