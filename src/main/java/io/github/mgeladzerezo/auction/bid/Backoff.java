package io.github.mgeladzerezo.auction.bid;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with full jitter: after the n-th failed attempt, sleep a uniformly random
 * time in {@code [0, min(cap, base * 2^(n-1))]}.
 *
 * <p>The jitter matters more than the exponent. Bidders that collided on version v all learn
 * about it at the same moment (when the winner commits); without jitter they would all re-read
 * and collide again on version v+1.
 */
public record Backoff(Duration base, Duration cap) {

    public Backoff {
        if (base.isNegative() || cap.compareTo(base) < 0) {
            throw new IllegalArgumentException("backoff needs 0 <= base <= cap");
        }
    }

    /** Upper bound of the sleep after the given number of failed attempts (1-based). */
    public Duration ceiling(int failedAttempts) {
        int shift = Math.min(Math.max(failedAttempts - 1, 0), 30);
        long nanos = base.toNanos() << shift;
        return nanos < 0 || nanos > cap.toNanos() ? cap : Duration.ofNanos(nanos);
    }

    /** A random sleep for the given number of failed attempts. */
    public Duration next(int failedAttempts) {
        long ceiling = ceiling(failedAttempts).toNanos();
        return ceiling == 0 ? Duration.ZERO : Duration.ofNanos(ThreadLocalRandom.current().nextLong(ceiling + 1));
    }
}
