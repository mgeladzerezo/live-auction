package io.github.mgeladzerezo.auction.load;

import io.github.mgeladzerezo.auction.load.StormBidder.Answer;
import io.github.mgeladzerezo.auction.load.StormBidder.Attempt;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * What one storm measured, and whether it was clean. Printed to the log and written to
 * {@code target/storm-<strategy>-<bidders>.txt} so the numbers quoted in the README can be
 * traced to a run.
 *
 * @param bidSeconds      time from the first bid sent to the last answer received
 * @param latencyP50Ms    client-side send-to-answer latency, over bids on an undisturbed connection
 * @param versionConflicts optimistic attempts that lost the version race (server meter)
 */
record StormReport(
        BidStorm.Scenario scenario,
        long attempts,
        long accepted,
        long rejected,
        long duplicateSends,
        long reconnects,
        long timeouts,
        long retryableErrors,
        double bidSeconds,
        double latencyP50Ms,
        double latencyP95Ms,
        double latencyP99Ms,
        double latencyMaxMs,
        long eventDeliveries,
        ServerMetrics server,
        StormVerifier.Outcome outcome,
        List<String> violations) {

    /** Meters read from the application after the run. */
    record ServerMetrics(long versionConflicts, long slowConsumerDisconnects, long resyncs,
                         Map<String, Long> answersByReason, double broadcastLagP50Ms, double broadcastLagP99Ms,
                         double broadcastLagMaxMs) {
    }

    private static final Logger log = LoggerFactory.getLogger(StormReport.class);

    static StormReport build(BidStorm.Scenario scenario, List<StormBidder> bidders, StormVerifier.Outcome outcome,
                             ServerMetrics server, List<String> violations) {
        long attempts = 0;
        long accepted = 0;
        long rejected = 0;
        long duplicateSends = 0;
        long firstSent = Long.MAX_VALUE;
        long lastAnswered = Long.MIN_VALUE;
        long deliveries = 0;
        List<Long> latencies = new ArrayList<>();
        for (StormBidder bidder : bidders) {
            deliveries += bidder.fingerprints().length;
            for (Attempt attempt : bidder.attempts) {
                List<Answer> answers = attempt.answersCopy();
                attempts++;
                duplicateSends += Math.max(0, attempt.sends - 1);
                if (answers.isEmpty()) {
                    continue;
                }
                if (answers.getFirst().outcome().equals("ACCEPTED")) {
                    accepted++;
                } else {
                    rejected++;
                }
                firstSent = Math.min(firstSent, attempt.firstSentNanos);
                lastAnswered = Math.max(lastAnswered, attempt.firstAnswerNanos);
                if (!attempt.disturbed) {
                    latencies.add(attempt.firstAnswerNanos - attempt.firstSentNanos);
                }
            }
        }
        long[] sorted = latencies.stream().mapToLong(Long::longValue).sorted().toArray();
        return new StormReport(scenario, attempts, accepted, rejected, duplicateSends,
                bidders.stream().mapToLong(b -> b.reconnects).sum(),
                bidders.stream().mapToLong(b -> b.timeouts).sum(),
                bidders.stream().mapToLong(b -> b.retryableErrors).sum(),
                attempts == 0 ? 0 : (lastAnswered - firstSent) / 1e9,
                percentileMs(sorted, 0.50), percentileMs(sorted, 0.95), percentileMs(sorted, 0.99),
                sorted.length == 0 ? 0 : sorted[sorted.length - 1] / 1e6,
                deliveries, server, outcome, List.copyOf(violations));
    }

    boolean clean() {
        return violations.isEmpty();
    }

    /** Answered bid attempts per second over the bidding period. */
    double attemptsPerSecond() {
        return bidSeconds == 0 ? 0 : (accepted + rejected) / bidSeconds;
    }

    double acceptedPerSecond() {
        return bidSeconds == 0 ? 0 : accepted / bidSeconds;
    }

    /** Version conflicts per bid attempt; zero by construction for the pessimistic strategy. */
    double conflictsPerAttempt() {
        return attempts == 0 ? 0 : (double) server.versionConflicts() / attempts;
    }

    void print() {
        String text = render();
        log.info("\n{}", text);
        try {
            Path directory = Path.of("target");
            if (Files.isDirectory(directory)) {
                Files.writeString(directory.resolve(
                        "storm-%s-%d.txt".formatted(scenario.strategy(), scenario.bidders())), text);
            }
        } catch (IOException e) {
            log.warn("Could not write the storm report: {}", e.toString());
        }
    }

    String render() {
        StringBuilder out = new StringBuilder();
        out.append("=== bid storm: %s locking, %d bidders on %d auctions ===%n"
                .formatted(scenario.strategy(), scenario.bidders(), scenario.auctions()));
        out.append("scenario             %ds auction, %ds anti-sniping window x %d, think time %d-%d ms,%n"
                .formatted(scenario.durationSeconds(), scenario.antiSnipeSeconds(), scenario.maxExtensions(),
                        scenario.minThinkMillis(), scenario.maxThinkMillis()));
        out.append("                     %.1f%% bids sent twice, %.1f%% connections dropped with a bid in flight%n"
                .formatted(scenario.duplicateRate() * 100, scenario.reconnectRate() * 100));
        out.append(String.format(Locale.ROOT, "bid attempts         %d in %.1f s = %.0f answered/s (%d accepted = %.0f/s, %d rejected)%n",
                attempts, bidSeconds, attemptsPerSecond(), accepted, acceptedPerSecond(), rejected));
        out.append(String.format(Locale.ROOT, "bid latency          p50 %.1f ms, p95 %.1f ms, p99 %.1f ms, max %.1f ms (client side, send to answer)%n",
                latencyP50Ms, latencyP95Ms, latencyP99Ms, latencyMaxMs));
        out.append(String.format(Locale.ROOT, "version conflicts    %d = %.3f per attempt (optimistic retries)%n",
                server.versionConflicts(), conflictsPerAttempt()));
        out.append("answers by reason    %s%n".formatted(server.answersByReason()));
        out.append(String.format(Locale.ROOT, "fan-out              %d event deliveries; broadcast lag p50 %.1f ms, p99 %.1f ms, max %.1f ms%n",
                eventDeliveries, server.broadcastLagP50Ms(), server.broadcastLagP99Ms(), server.broadcastLagMaxMs()));
        out.append("client chaos         %d duplicate sends, %d reconnects, %d answer timeouts, %d retryable errors%n"
                .formatted(duplicateSends, reconnects, timeouts, retryableErrors));
        out.append("server               %d slow-consumer disconnects, %d subscription resyncs%n"
                .formatted(server.slowConsumerDisconnects(), server.resyncs()));
        outcome.auctions().values().forEach(a -> out.append(
                "auction %-12d %s, %d bids, %d events, %d extensions, final price %s, %d subscribers%n"
                        .formatted(a.id(), a.status(), a.bids(), a.events(), a.extensions(), a.finalPrice(),
                                a.subscribers())));
        out.append("--- invariants checked ---%n".formatted());
        outcome.checks().forEach(check -> out.append("  ").append(check).append(System.lineSeparator()));
        if (violations.isEmpty()) {
            out.append("RESULT: clean, no invariant violated%n".formatted());
        } else {
            out.append("RESULT: %d VIOLATIONS%n".formatted(violations.size()));
            violations.stream().limit(40).forEach(v -> out.append("  ! ").append(v).append(System.lineSeparator()));
        }
        return out.toString();
    }

    private static double percentileMs(long[] sortedNanos, double percentile) {
        if (sortedNanos.length == 0) {
            return 0;
        }
        int index = (int) Math.ceil(percentile * sortedNanos.length) - 1;
        return sortedNanos[Math.clamp(index, 0, sortedNanos.length - 1)] / 1e6;
    }

    /** Side-by-side summary of several runs, for the strategy comparison. */
    static String comparison(List<StormReport> reports) {
        StringBuilder out = new StringBuilder();
        out.append(String.format(Locale.ROOT, "%-12s %9s %11s %11s %9s %9s %9s %13s %10s%n", "strategy", "attempts",
                "answered/s", "accepted/s", "p50 ms", "p99 ms", "max ms", "conflicts/bid", "CONTENTION"));
        for (StormReport report : reports) {
            out.append(String.format(Locale.ROOT, "%-12s %9d %11.0f %11.0f %9.1f %9.1f %9.1f %13.3f %10d%n",
                    report.scenario.strategy(), report.attempts, report.attemptsPerSecond(),
                    report.acceptedPerSecond(), report.latencyP50Ms, report.latencyP99Ms, report.latencyMaxMs,
                    report.conflictsPerAttempt(), report.server.answersByReason().getOrDefault("CONTENTION", 0L)));
        }
        return out.toString();
    }

    @Override
    public String toString() {
        return Arrays.toString(new Object[] {scenario, attempts, accepted, rejected, violations.size()});
    }
}
