package io.github.mgeladzerezo.auction.load;

import io.github.mgeladzerezo.auction.auction.AuctionRepository;
import io.github.mgeladzerezo.auction.auction.ConsistencyAudit;
import io.github.mgeladzerezo.auction.support.Api;
import io.github.mgeladzerezo.auction.support.TestApp;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.JsonNode;

/**
 * The load test behind the "no bid is lost" claim.
 *
 * <p>Starts the real application (HTTP port, WebSocket endpoint, scheduler, LISTEN connection)
 * against the shared Testcontainers PostgreSQL, registers the bidders through the REST API,
 * connects each of them with its own WebSocket and lets them all bid on a few hot auctions from
 * the moment those open until the scheduler closes them, through every anti-sniping extension.
 * Afterwards {@link StormVerifier} checks the invariants from both sides: what the clients were
 * told, and what the database contains.
 *
 * <p>Clients and server share one JVM and one machine, so the throughput and latency figures
 * are a lower bound for the server, not a benchmark of it.
 */
final class BidStorm {

    /**
     * @param startsInSeconds  delay before the auctions open; the bidders connect in this time
     * @param durationSeconds  auction length before extensions
     * @param antiSnipeSeconds anti-sniping window; each extension adds this much
     * @param duplicateRate    fraction of bids sent twice in a row with the same client id
     * @param reconnectRate    fraction of bids after which the client drops its connection with
     *                         the bid in flight, reconnects with lastSeq and re-sends
     */
    record Scenario(String strategy, int bidders, int auctions, int startsInSeconds, int durationSeconds,
                    int antiSnipeSeconds, int maxExtensions, int minThinkMillis, int maxThinkMillis,
                    double duplicateRate, double reconnectRate, long seed) {

        /** 200 bidders; small enough to run in every build. */
        static Scenario smoke(String strategy) {
            return new Scenario(strategy, 200, 3, 4, 4, 2, 3, 40, 160, 0.03, 0.01, 7);
        }

        /** 1,000 bidders on three auctions; behind the {@code load} Maven profile. */
        static Scenario load(String strategy) {
            return new Scenario(strategy, Integer.getInteger("storm.bidders", 1000), 3, 12, 8, 3, 5,
                    Integer.getInteger("storm.minThinkMillis", 100), Integer.getInteger("storm.maxThinkMillis", 400),
                    0.02, 0.005, 7);
        }

        Duration expectedRunTime() {
            return Duration.ofSeconds(startsInSeconds + durationSeconds + (long) antiSnipeSeconds * maxExtensions);
        }
    }

    /** A registered bidder account. */
    private record Account(long userId, String token) {
    }

    static final long START_PRICE = 1000;
    static final long INCREMENT = 100;

    private static final Logger log = LoggerFactory.getLogger(BidStorm.class);
    private static final int HTTP_CLIENTS = 8;
    private static final int CONNECT_CONCURRENCY = 48;

    private BidStorm() {
    }

    /** Runs the scenario to completion and returns the measurements and any invariant violations. */
    static StormReport run(Scenario scenario) throws Exception {
        try (TestApp app = TestApp.start("storm-" + scenario.strategy(),
                "auction.bidding.strategy=" + scenario.strategy(),
                // A reconnecting client must always be replayable in this test, never re-snapshotted.
                "auction.realtime.replay-limit=20000",
                "spring.datasource.hikari.connection-timeout=30000");
             ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor()) {

            Api api = new Api(app.port());
            String run = Long.toString(System.nanoTime(), 36);
            List<Account> accounts = registerBidders(api, threads, scenario, run);
            List<Long> auctionIds = createAuctions(api, scenario, run);
            log.info("[{}] {} bidders registered, auctions {} open in {} s", scenario.strategy(),
                    accounts.size(), auctionIds, scenario.startsInSeconds());

            List<HttpClient> clients = new ArrayList<>();
            for (int i = 0; i < HTTP_CLIENTS; i++) {
                clients.add(HttpClient.newBuilder().executor(threads).build());
            }
            URI endpoint = URI.create("ws://localhost:" + app.port() + "/ws");
            CountDownLatch startGate = new CountDownLatch(1);
            List<StormBidder> bidders = new ArrayList<>();
            for (int i = 0; i < accounts.size(); i++) {
                bidders.add(new StormBidder(i, accounts.get(i).userId(), accounts.get(i).token(),
                        auctionIds.get(i % auctionIds.size()), endpoint, clients.get(i % HTTP_CLIENTS),
                        scenario, startGate));
            }

            connectAll(bidders, threads);
            long subscribedBeforeOpen = bidders.stream().filter(b -> b.firstSeq == 1).count();
            log.info("[{}] all {} sockets subscribed ({} before the auctions opened); bidding", scenario.strategy(),
                    bidders.size(), subscribedBeforeOpen);

            List<String> failures = new ArrayList<>();
            List<Future<?>> loops = new ArrayList<>();
            Duration settleLimit = scenario.expectedRunTime().plusSeconds(120);
            for (StormBidder bidder : bidders) {
                loops.add(threads.submit(() -> {
                    bidder.bidUntilClosed();
                    if (!bidder.awaitSettled(settleLimit)) {
                        throw new IllegalStateException("never saw AUCTION_SETTLED");
                    }
                    return null;
                }));
            }
            startGate.countDown();
            for (int i = 0; i < loops.size(); i++) {
                try {
                    loops.get(i).get(settleLimit.toSeconds() + 60, TimeUnit.SECONDS);
                } catch (Exception e) {
                    failures.add("bidder " + i + " failed: " + e);
                }
            }
            awaitTrailingAnswers(bidders);
            bidders.forEach(StormBidder::close);

            StormVerifier verifier = new StormVerifier(scenario, app.bean(JdbcClient.class),
                    app.bean(AuctionRepository.class), app.bean(ConsistencyAudit.class));
            StormVerifier.Outcome outcome = verifier.verify(auctionIds, bidders);
            failures.addAll(outcome.violations());

            StormReport report = StormReport.build(scenario, bidders, outcome, serverMetrics(app, scenario), failures);
            report.print();
            clients.forEach(HttpClient::shutdownNow);
            return report;
        }
    }

    private static List<Account> registerBidders(Api api, ExecutorService threads, Scenario scenario, String run)
            throws Exception {
        Semaphore limit = new Semaphore(32);
        List<Future<Account>> pending = new ArrayList<>();
        for (int i = 0; i < scenario.bidders(); i++) {
            String username = "storm_" + run + "_" + i;
            pending.add(threads.submit(() -> {
                limit.acquire();
                try {
                    JsonNode registered = api.register(username);
                    return new Account(registered.path("userId").asLong(), registered.path("token").asString());
                } finally {
                    limit.release();
                }
            }));
        }
        List<Account> accounts = new ArrayList<>();
        for (Future<Account> account : pending) {
            accounts.add(account.get(120, TimeUnit.SECONDS));
        }
        return accounts;
    }

    /** Creates the hot auctions through the REST API. The last one has a reserve nobody will reach. */
    private static List<Long> createAuctions(Api api, Scenario scenario, String run) {
        String sellerToken = api.register("seller_" + run).path("token").asString();
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < scenario.auctions(); i++) {
            boolean unreachableReserve = scenario.auctions() > 1 && i == scenario.auctions() - 1;
            Api.Response created = api.post("/api/auctions", sellerToken, """
                    {"title":"Storm lot %d","startPrice":%d,"minIncrement":%d,%s
                     "startsInSeconds":%d,"durationSeconds":%d,"antiSnipeWindowSeconds":%d,"maxExtensions":%d}
                    """.formatted(i, START_PRICE, INCREMENT,
                    unreachableReserve ? "\"reservePrice\":100000000000000," : "",
                    scenario.startsInSeconds(), scenario.durationSeconds(), scenario.antiSnipeSeconds(),
                    scenario.maxExtensions()));
            if (created.status() != 201) {
                throw new IllegalStateException("Could not create auction: " + created);
            }
            ids.add(created.body().path("id").asLong());
        }
        return ids;
    }

    private static void connectAll(List<StormBidder> bidders, ExecutorService threads) throws Exception {
        Semaphore limit = new Semaphore(CONNECT_CONCURRENCY);
        List<Future<?>> pending = new ArrayList<>();
        for (StormBidder bidder : bidders) {
            pending.add(threads.submit(() -> {
                limit.acquire();
                try {
                    bidder.connect();
                } finally {
                    limit.release();
                }
                return null;
            }));
        }
        for (Future<?> connection : pending) {
            connection.get(120, TimeUnit.SECONDS);
        }
    }

    /**
     * A bid that was deliberately sent twice gets two answers, and the second may still be on
     * its way when the bidder has already seen the auction settle. Waits until every answer
     * owed on a healthy connection has arrived (or gives up and lets the verifier report it).
     */
    private static void awaitTrailingAnswers(List<StormBidder> bidders) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            boolean complete = bidders.stream().allMatch(StormBidder::hasEveryOwedAnswer);
            if (complete) {
                return;
            }
            Thread.sleep(100);
        }
    }

    /** Reads the server's own meters for the run: conflicts, lag, slow consumers. */
    private static StormReport.ServerMetrics serverMetrics(TestApp app, Scenario scenario) {
        MeterRegistry registry = app.bean(MeterRegistry.class);
        Counter retries = registry.find("auction.bid.retries").tag("strategy", scenario.strategy()).counter();
        Counter slowConsumers = registry.find("auction.ws.slow.consumers").counter();
        Counter resyncs = registry.find("auction.ws.resyncs").counter();
        Map<String, Long> byReason = new TreeMap<>();
        for (Counter counter : registry.find("auction.bids").counters()) {
            String outcome = counter.getId().getTag("outcome");
            String reason = counter.getId().getTag("reason");
            byReason.merge("ACCEPTED".equals(outcome) ? "ACCEPTED" : reason, (long) counter.count(), Long::sum);
        }
        Timer lag = registry.find("auction.broadcast.lag").timer();
        double lagP50 = 0;
        double lagP99 = 0;
        if (lag != null) {
            for (ValueAtPercentile value : lag.takeSnapshot().percentileValues()) {
                if (value.percentile() == 0.5) {
                    lagP50 = value.value(TimeUnit.MILLISECONDS);
                } else if (value.percentile() == 0.99) {
                    lagP99 = value.value(TimeUnit.MILLISECONDS);
                }
            }
        }
        return new StormReport.ServerMetrics(
                retries == null ? 0 : (long) retries.count(),
                slowConsumers == null ? 0 : (long) slowConsumers.count(),
                resyncs == null ? 0 : (long) resyncs.count(),
                byReason, lagP50, lagP99, lag == null ? 0 : lag.max(TimeUnit.MILLISECONDS));
    }
}
