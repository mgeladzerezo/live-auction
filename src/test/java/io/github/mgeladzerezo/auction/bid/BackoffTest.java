package io.github.mgeladzerezo.auction.bid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class BackoffTest {

    private final Backoff backoff = new Backoff(Duration.ofMillis(2), Duration.ofMillis(50));

    @Test
    void ceilingDoublesPerFailedAttemptUpToTheCap() {
        assertThat(backoff.ceiling(1)).isEqualTo(Duration.ofMillis(2));
        assertThat(backoff.ceiling(2)).isEqualTo(Duration.ofMillis(4));
        assertThat(backoff.ceiling(3)).isEqualTo(Duration.ofMillis(8));
        assertThat(backoff.ceiling(5)).isEqualTo(Duration.ofMillis(32));
        assertThat(backoff.ceiling(6)).isEqualTo(Duration.ofMillis(50));
        assertThat(backoff.ceiling(1000)).isEqualTo(Duration.ofMillis(50));
    }

    @Test
    void sleepsAreJitteredWithinTheCeiling() {
        Set<Duration> seen = new HashSet<>();
        for (int i = 0; i < 2000; i++) {
            Duration sleep = backoff.next(3);
            assertThat(sleep).isBetween(Duration.ZERO, Duration.ofMillis(8));
            seen.add(sleep);
        }
        // Full jitter: the values spread over the range instead of clustering on one delay.
        assertThat(seen.size()).isGreaterThan(1000);
    }

    @Test
    void zeroBaseMeansNoSleep() {
        assertThat(new Backoff(Duration.ZERO, Duration.ZERO).next(4)).isEqualTo(Duration.ZERO);
    }

    @Test
    void rejectsCapBelowBase() {
        assertThatThrownBy(() -> new Backoff(Duration.ofMillis(10), Duration.ofMillis(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
