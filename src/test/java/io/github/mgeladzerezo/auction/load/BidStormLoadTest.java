package io.github.mgeladzerezo.auction.load;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The full proof and the benchmark: 1,000 concurrent WebSocket bidders, both locking
 * strategies, same scenario, same machine, one after the other.
 *
 * <p>Run with {@code ./mvnw -Pload verify}. Tagged {@code load} and excluded from the default
 * build because it takes minutes and wants a larger heap. Bidder count and think time can be
 * overridden with {@code -Dstorm.bidders}, {@code -Dstorm.minThinkMillis} and
 * {@code -Dstorm.maxThinkMillis}; {@code -Dstorm.strategies=optimistic} runs a single strategy.
 */
@Tag("load")
class BidStormLoadTest {

    private static final Logger log = LoggerFactory.getLogger(BidStormLoadTest.class);

    @Test
    void noBidIsLostWith1000ConcurrentBiddersUnderEitherStrategy() throws Exception {
        List<StormReport> reports = new ArrayList<>();
        for (String strategy : System.getProperty("storm.strategies", "optimistic,pessimistic").split(",")) {
            reports.add(BidStorm.run(BidStorm.Scenario.load(strategy)));
        }

        String comparison = StormReport.comparison(reports);
        log.info("\n=== strategy comparison ===\n{}", comparison);
        if (Files.isDirectory(Path.of("target"))) {
            Files.writeString(Path.of("target", "storm-comparison.txt"), comparison);
        }
        for (StormReport report : reports) {
            assertThat(report.violations()).as(report.render()).isEmpty();
        }
    }
}
