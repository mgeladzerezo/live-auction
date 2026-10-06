package io.github.mgeladzerezo.auction.load;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The always-on version of the load test: 200 concurrent WebSocket bidders on three auctions,
 * through the anti-sniping window and the close, once per locking strategy. Small enough for
 * every {@code mvn verify}, large enough that the build fails if a bid can be lost, answered
 * twice, accepted out of order or after the deadline, or if clients can see different streams.
 *
 * <p>The 1,000-bidder run with the published numbers is {@link BidStormLoadTest}.
 */
class BidStormSmokeTest {

    @ParameterizedTest
    @ValueSource(strings = {"optimistic", "pessimistic"})
    void noBidIsLostWith200ConcurrentBidders(String strategy) throws Exception {
        StormReport report = BidStorm.run(BidStorm.Scenario.smoke(strategy));

        assertThat(report.violations()).as(report.render()).isEmpty();
        assertThat(report.attempts()).as("the storm actually stormed").isGreaterThan(1000);
        assertThat(report.accepted()).isGreaterThan(50);
        if (strategy.equals("pessimistic")) {
            assertThat(report.server().versionConflicts()).as("a row lock leaves nothing to conflict on").isZero();
        }
    }
}
