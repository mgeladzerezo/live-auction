package io.github.mgeladzerezo.auction.bid;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mgeladzerezo.auction.auction.Auction;
import io.github.mgeladzerezo.auction.auction.AuctionStatus;
import io.github.mgeladzerezo.auction.auction.ConsistencyAudit;
import io.github.mgeladzerezo.auction.auth.User;
import io.github.mgeladzerezo.auction.support.AbstractIntegrationTest;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntToLongFunction;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Deterministic concurrency tests. Each one forces a specific interleaving with latches
 * (through {@link BidCheckpoint}, or by holding a transaction open) instead of hoping a load
 * test stumbles on it, and then asserts who must win.
 */
class ConcurrencyRaceTest extends AbstractIntegrationTest {

    private static final long WAIT_SECONDS = 20;

    @Autowired
    List<BidStrategy> strategies;
    @Autowired
    ConsistencyAudit audit;
    @Autowired
    MeterRegistry meters;
    @Autowired
    DataSource dataSource;
    @Autowired
    PlatformTransactionManager transactionManager;

    private final ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor();

    @AfterEach
    void stopThreads() {
        threads.shutdownNow();
    }

    private BidStrategy strategy(String name) {
        return strategies.stream().filter(s -> s.name().equals(name)).findFirst().orElseThrow();
    }

    private double optimisticRetries() {
        return meters.counter("auction.bid.retries", "strategy", "optimistic").count();
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for the other side of the interleaving");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static <T> T get(Future<T> future) throws Exception {
        return future.get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    // ---- extension race ---------------------------------------------------------------------

    /**
     * Two bids read the same auction version inside the anti-sniping window, so both decide
     * "I extend the deadline". Only one write may win; the other must be re-decided against
     * the already extended deadline and must not extend it a second time.
     */
    @Test
    void twoBidsThatBothDecidedToExtendProduceExactlyOneExtension() throws Exception {
        Auction auction = open(auction().duration(Duration.ofSeconds(20)).antiSniping(30, 5));
        User alice = newUser("alice");
        User bob = newUser("bob");
        BidCommand first = bid(auction, alice, 1000);
        BidCommand second = bid(auction, bob, 1500);

        CyclicBarrier bothHaveRead = new CyclicBarrier(2);
        CountDownLatch firstCommitted = new CountDownLatch(1);
        Set<String> alreadyPaused = ConcurrentHashMap.newKeySet();
        ledger.installCheckpoint(new BidCheckpoint() {
            @Override
            public void afterRead(BidCommand command) {
                if (!alreadyPaused.add(command.clientBidId())) {
                    return; // a retry: let it run
                }
                try {
                    bothHaveRead.await(WAIT_SECONDS, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
                if (command == second) {
                    await(firstCommitted); // hold the stale decision until the other bid is in
                }
            }
        });
        double retriesBefore = optimisticRetries();

        BidStrategy optimistic = strategy("optimistic");
        Future<BidResult> firstResult = threads.submit(() -> {
            try {
                return optimistic.place(first);
            } finally {
                firstCommitted.countDown();
            }
        });
        Future<BidResult> secondResult = threads.submit(() -> optimistic.place(second));

        BidResult.Accepted firstAccepted = (BidResult.Accepted) get(firstResult);
        BidResult.Accepted secondAccepted = (BidResult.Accepted) get(secondResult);

        assertThat(firstAccepted.extendedTo()).isEqualTo(auction.endsAt().plusSeconds(30));
        assertThat(secondAccepted.extendedTo()).as("the re-decided bid sees 50 s left").isNull();
        assertThat(optimisticRetries() - retriesBefore).isEqualTo(1.0);
        Auction after = reload(auction.id());
        assertThat(after.extensionCount()).isEqualTo(1);
        assertThat(after.endsAt()).isEqualTo(auction.endsAt().plusSeconds(30));
        assertThat(after.currentPrice()).isEqualTo(1500);
        assertThat(after.leaderId()).isEqualTo(bob.id());
        assertThat(after.lastSeq()).isEqualTo(auction.lastSeq() + 3); // bid, extension, bid
        assertThat(audit.check(auction.id()).violations()).isEmpty();
    }

    /** The same race under the pessimistic strategy: the second bid simply waits its turn. */
    @Test
    void pessimisticBidWaitsForTheRowLockAndIsDecidedAgainstTheCommittedState() throws Exception {
        Auction auction = open(auction().duration(Duration.ofSeconds(20)).antiSniping(30, 5));
        User alice = newUser("alice");
        User bob = newUser("bob");
        BidCommand first = bid(auction, alice, 1000);
        BidCommand second = bid(auction, bob, 1500);

        CountDownLatch firstWritten = new CountDownLatch(1);
        CountDownLatch letFirstCommit = new CountDownLatch(1);
        ledger.installCheckpoint(new BidCheckpoint() {
            @Override
            public void beforeCommit(BidCommand command) {
                if (command == first) {
                    firstWritten.countDown();
                    await(letFirstCommit);
                }
            }
        });

        BidStrategy pessimistic = strategy("pessimistic");
        Future<BidResult> firstResult = threads.submit(() -> pessimistic.place(first));
        await(firstWritten);
        Future<BidResult> secondResult = threads.submit(() -> pessimistic.place(second));

        // The first transaction holds the row lock: the second cannot even read.
        assertStillBlocked(secondResult);
        letFirstCommit.countDown();

        assertThat(((BidResult.Accepted) get(firstResult)).extendedTo()).isNotNull();
        assertThat(((BidResult.Accepted) get(secondResult)).extendedTo()).isNull();
        Auction after = reload(auction.id());
        assertThat(after.extensionCount()).isEqualTo(1);
        assertThat(after.currentPrice()).isEqualTo(1500);
        assertThat(audit.check(auction.id()).violations()).isEmpty();
    }

    // ---- close versus bid -------------------------------------------------------------------

    /**
     * A bid reads the auction just before the deadline, then stalls. The deadline passes and
     * the scheduler closes the auction. The bid's write must find the version gone, re-read,
     * and be rejected: a bid loses to a committed close.
     */
    @Test
    void bidThatDecidedBeforeTheDeadlineLosesToACloseCommittedBeforeItsWrite() throws Exception {
        Auction auction = open(auction().duration(Duration.ofMillis(1500)));
        User alice = newUser("alice");
        BidCommand command = bid(auction, alice, 1000);

        CountDownLatch decided = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        ledger.installCheckpoint(new BidCheckpoint() {
            @Override
            public void afterRead(BidCommand paused) {
                if (decided.getCount() > 0) {
                    decided.countDown();
                    await(closed);
                }
            }
        });
        double retriesBefore = optimisticRetries();

        Future<BidResult> result = threads.submit(() -> strategy("optimistic").place(command));
        await(decided);
        assertThat(dbNow()).as("the bid made its decision while the auction was still running")
                .isBefore(auction.endsAt());

        awaitDbTime(auction.endsAt());
        assertThat(lifecycle.closeDue()).isEqualTo(1);
        closed.countDown();

        assertThat(get(result)).isInstanceOfSatisfying(BidResult.Rejected.class,
                rejected -> assertThat(rejected.reason()).isEqualTo(RejectReason.AUCTION_CLOSED));
        assertThat(optimisticRetries() - retriesBefore).isEqualTo(1.0);
        Auction after = reload(auction.id());
        assertThat(after.status()).isEqualTo(AuctionStatus.CLOSED);
        assertThat(after.currentPrice()).isNull();
        assertThat(count("bids", auction.id())).isZero();
        assertThat(audit.check(auction.id()).violations()).isEmpty();
    }

    /**
     * A bid inside the anti-sniping window has written its extension but not committed when the
     * original deadline passes. The closer finds the auction due and queues for its row. When
     * the bid commits, the closer gets the row, re-checks the deadline against what the bid
     * wrote, and must leave the auction open: the close loses to a bid that committed an
     * extension.
     */
    @ParameterizedTest
    @ValueSource(strings = {"optimistic", "pessimistic"})
    void closeLosesToABidThatCommitsAnExtension(String name) throws Exception {
        Auction auction = open(auction().duration(Duration.ofMillis(1500)).antiSniping(2, 3));
        User alice = newUser("alice");
        BidCommand command = bid(auction, alice, 1000);

        CountDownLatch written = new CountDownLatch(1);
        CountDownLatch letCommit = new CountDownLatch(1);
        ledger.installCheckpoint(new BidCheckpoint() {
            @Override
            public void beforeCommit(BidCommand paused) {
                written.countDown();
                await(letCommit);
            }
        });

        Future<BidResult> result = threads.submit(() -> strategy(name).place(command));
        await(written);
        awaitDbTime(auction.endsAt());

        // Deadline passed, bid written but uncommitted. The closer sees a due auction (it cannot
        // see the uncommitted extension) and waits for the row instead of closing or skipping it.
        Future<Integer> closing = threads.submit(() -> lifecycle.closeDue());
        assertStillBlocked(closing);
        // A second scheduler instance does not pile up behind the first: the auction is claimed.
        long started = System.nanoTime();
        assertThat(lifecycle.closeDue()).isZero();
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(2));
        assertThat(reload(auction.id()).status()).isEqualTo(AuctionStatus.OPEN);

        letCommit.countDown();
        BidResult.Accepted accepted = (BidResult.Accepted) get(result);
        Instant extendedEnd = auction.endsAt().plusSeconds(2);
        assertThat(accepted.extendedTo()).isEqualTo(extendedEnd);

        // The waiting closer now has the row, re-checks, and finds the deadline moved.
        assertThat(get(closing)).as("the close must lose to the committed extension").isZero();
        assertThat(reload(auction.id()).status()).isEqualTo(AuctionStatus.OPEN);
        assertThat(lifecycle.closeDue()).isZero();

        awaitDbTime(extendedEnd);
        assertThat(lifecycle.closeDue()).isEqualTo(1);
        lifecycle.settleClosed();
        Auction settled = reload(auction.id());
        assertThat(settled.status()).isEqualTo(AuctionStatus.SETTLED);
        assertThat(settled.leaderId()).isEqualTo(alice.id());
        assertThat(audit.check(auction.id()).violations()).isEmpty();
    }

    /**
     * The same race where the bid does not extend (anti-sniping off): the closer waits for the
     * bid and then closes the auction with that bid as the winner. And it waits rather than
     * skips: skipping locked rows is what starved the closer behind pessimistic bidders.
     */
    @Test
    void closerQueuesBehindAnInFlightBidAndThenClosesWithThatBidAsWinner() throws Exception {
        Auction auction = open(auction().duration(Duration.ofMillis(1500)));
        User alice = newUser("alice");
        BidCommand command = bid(auction, alice, 1000);

        CountDownLatch written = new CountDownLatch(1);
        CountDownLatch letCommit = new CountDownLatch(1);
        ledger.installCheckpoint(new BidCheckpoint() {
            @Override
            public void beforeCommit(BidCommand paused) {
                written.countDown();
                await(letCommit);
            }
        });

        Future<BidResult> result = threads.submit(() -> strategy("pessimistic").place(command));
        await(written);
        awaitDbTime(auction.endsAt());

        Future<Integer> closing = threads.submit(() -> lifecycle.closeDue());
        assertStillBlocked(closing);
        letCommit.countDown();

        assertThat(get(result)).isInstanceOf(BidResult.Accepted.class);
        assertThat(get(closing)).isEqualTo(1);
        Auction closed = reload(auction.id());
        assertThat(closed.status()).isEqualTo(AuctionStatus.CLOSED);
        assertThat(closed.leaderId()).as("the bid that was decided before the deadline counts").isEqualTo(alice.id());
        assertThat(closed.currentPrice()).isEqualTo(1000);
        lifecycle.settleClosed();
        assertThat(reload(auction.id()).status()).isEqualTo(AuctionStatus.SETTLED);
        assertThat(audit.check(auction.id()).violations()).isEmpty();
    }

    /**
     * Pessimistic counterpart of "a bid loses to a committed close": the close is in flight
     * (row locked, not committed), the bid queues on the lock, and when it finally gets the row
     * it reads CLOSED.
     */
    @Test
    void pessimisticBidQueuedBehindAnUncommittedCloseIsRejectedOnceItCommits() throws Exception {
        Auction auction = open(auction().duration(Duration.ofMillis(300)));
        User alice = newUser("alice");
        awaitDbTime(auction.endsAt());

        CountDownLatch closeWritten = new CountDownLatch(1);
        CountDownLatch letCloseCommit = new CountDownLatch(1);
        // closeDue() joins this outer transaction, so its row lock is held until we release it.
        Future<Integer> closing = threads.submit(() -> new TransactionTemplate(transactionManager).execute(status -> {
            int closedCount = lifecycle.closeDue();
            closeWritten.countDown();
            await(letCloseCommit);
            return closedCount;
        }));
        await(closeWritten);

        Future<BidResult> result = threads.submit(() -> strategy("pessimistic").place(bid(auction, alice, 1000)));
        assertStillBlocked(result);
        letCloseCommit.countDown();

        assertThat(get(closing)).isEqualTo(1);
        assertThat(get(result)).isInstanceOfSatisfying(BidResult.Rejected.class,
                rejected -> assertThat(rejected.reason()).isEqualTo(RejectReason.AUCTION_CLOSED));
        assertThat(count("bids", auction.id())).isZero();
    }

    /**
     * The pessimistic read must take its clock reading after the row lock is granted, not when
     * the statement started. Here the bid starts before the deadline, waits on a lock until
     * after it, and nothing else changes: only a clock read after the wait rejects it.
     */
    @Test
    void pessimisticBidThatWaitedPastTheDeadlineIsJudgedByTheClockAfterTheWait() throws Exception {
        Auction auction = open(auction().duration(Duration.ofMillis(1500)));
        User alice = newUser("alice");

        Future<BidResult> result;
        try (Connection blocker = dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            try (PreparedStatement lock = blocker.prepareStatement("SELECT id FROM auctions WHERE id = ? FOR UPDATE")) {
                lock.setLong(1, auction.id());
                lock.execute();
            }
            assertThat(dbNow()).isBefore(auction.endsAt());
            result = threads.submit(() -> strategy("pessimistic").place(bid(auction, alice, 1000)));
            assertStillBlocked(result);
            awaitDbTime(auction.endsAt());
            blocker.commit(); // releases the lock without having changed the row
        }

        assertThat(get(result)).isInstanceOfSatisfying(BidResult.Rejected.class,
                rejected -> assertThat(rejected.reason()).isEqualTo(RejectReason.AUCTION_CLOSED));
        assertThat(reload(auction.id()).status()).as("no one closed it; only the clock did")
                .isEqualTo(AuctionStatus.OPEN);
    }

    // ---- bounded retry ----------------------------------------------------------------------

    /**
     * Every attempt finds that the row moved after it was read. After the configured number of
     * attempts the bid must get a definitive CONTENTION rejection rather than spin forever, and
     * that answer is as idempotent as any other.
     */
    @Test
    void optimisticBidGivesUpWithContentionAfterTheConfiguredAttempts() {
        Auction auction = open(auction());
        User alice = newUser("alice");
        BidCommand command = bid(auction, alice, 1000);
        ledger.installCheckpoint(new BidCheckpoint() {
            @Override
            public void afterRead(BidCommand paused) {
                bumpVersionOnAnotherConnection(auction.id());
            }
        });
        double retriesBefore = optimisticRetries();

        BidResult result = strategy("optimistic").place(command);

        assertThat(result).isInstanceOfSatisfying(BidResult.Rejected.class, rejected -> {
            assertThat(rejected.reason()).isEqualTo(RejectReason.CONTENTION);
            assertThat(rejected.duplicate()).isFalse();
        });
        assertThat(optimisticRetries() - retriesBefore).as("default maxAttempts").isEqualTo(8.0);
        assertThat(count("bids", auction.id())).isZero();
        assertThat(reload(auction.id()).currentPrice()).isNull();

        ledger.installCheckpoint(BidCheckpoint.NONE);
        assertThat(strategy("optimistic").place(command)).isInstanceOfSatisfying(BidResult.Rejected.class, replayed -> {
            assertThat(replayed.reason()).isEqualTo(RejectReason.CONTENTION);
            assertThat(replayed.duplicate()).isTrue();
        });
    }

    // ---- stampedes --------------------------------------------------------------------------

    /** Many bidders offer the same amount at the same instant: exactly one can be accepted. */
    @ParameterizedTest
    @ValueSource(strings = {"optimistic", "pessimistic"})
    void identicalSimultaneousBidsYieldExactlyOneWinner(String name) throws Exception {
        Auction auction = open(auction());
        int bidders = 24;
        List<BidResult> results = stampede(name, auction, bidders, i -> 1000L);

        assertThat(results).hasSize(bidders);
        assertThat(results.stream().filter(BidResult.Accepted.class::isInstance)).hasSize(1);
        assertThat(results.stream().filter(BidResult.Rejected.class::isInstance)
                .map(r -> ((BidResult.Rejected) r).reason()))
                .hasSize(bidders - 1)
                .containsOnly(RejectReason.TOO_LOW);
        assertThat(count("bids", auction.id())).isEqualTo(1);
        assertThat(count("bid_attempts", auction.id())).isEqualTo(bidders);
        assertThat(audit.check(auction.id()).violations()).isEmpty();
    }

    /** Many bidders with different amounts: whatever the order, the history must be a valid ladder. */
    @ParameterizedTest
    @ValueSource(strings = {"optimistic", "pessimistic"})
    void simultaneousBidsOfDifferentAmountsLeaveAValidLadder(String name) throws Exception {
        Auction auction = open(auction().antiSniping(3600, 4));
        int bidders = 32;
        List<BidResult> results = stampede(name, auction, bidders, i -> 1000L + 100L * i);

        long accepted = results.stream().filter(BidResult.Accepted.class::isInstance).count();
        long highestAccepted = results.stream().filter(BidResult.Accepted.class::isInstance)
                .mapToLong(BidResult::amount).max().orElseThrow();
        Auction after = reload(auction.id());
        assertThat(after.bidCount()).isEqualTo((int) accepted);
        assertThat(after.currentPrice()).isEqualTo(highestAccepted);
        assertThat(after.extensionCount()).as("every bid was inside the window, but only one may extend").isEqualTo(1);
        assertThat(count("bid_attempts", auction.id())).isEqualTo(bidders);
        assertThat(audit.check(auction.id()).violations()).isEmpty();
    }

    /** The same command submitted twice at once (a client retrying over a second connection). */
    @ParameterizedTest
    @ValueSource(strings = {"optimistic", "pessimistic"})
    void sameCommandSubmittedConcurrentlyCreatesOneBidAndOneAnswer(String name) throws Exception {
        Auction auction = open(auction());
        User alice = newUser("alice");
        BidCommand command = bid(auction, alice, 1000);
        BidStrategy strategy = strategy(name);

        CountDownLatch start = new CountDownLatch(1);
        List<Future<BidResult>> futures = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            futures.add(threads.submit(() -> {
                await(start);
                return strategy.place(command);
            }));
        }
        start.countDown();

        List<BidResult.Accepted> answers = new ArrayList<>();
        for (Future<BidResult> future : futures) {
            answers.add((BidResult.Accepted) get(future));
        }
        assertThat(answers.stream().map(BidResult.Accepted::bidId).distinct()).hasSize(1);
        assertThat(answers.stream().filter(a -> !a.duplicate())).as("exactly one computed the answer").hasSize(1);
        assertThat(count("bids", auction.id())).isEqualTo(1);
        assertThat(count("bid_attempts", auction.id())).isEqualTo(1);
        assertThat(reload(auction.id()).bidCount()).isEqualTo(1);
        assertThat(audit.check(auction.id()).violations()).isEmpty();
    }

    // ---- several schedulers -----------------------------------------------------------------

    /** Eight "instances" run the closer at once over the same due auctions. */
    @Test
    void concurrentClosersCloseEveryDueAuctionExactlyOnce() throws Exception {
        List<Auction> due = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            due.add(open(auction().duration(Duration.ofMillis(300))));
        }
        awaitDbTime(due.getLast().endsAt());

        CountDownLatch start = new CountDownLatch(1);
        List<Future<Integer>> closers = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            closers.add(threads.submit(() -> {
                await(start);
                int closedCount = 0;
                for (int pass = 0; pass < 3; pass++) {
                    closedCount += lifecycle.closeDue();
                }
                return closedCount;
            }));
        }
        start.countDown();

        int totalClosed = 0;
        for (Future<Integer> closer : closers) {
            totalClosed += get(closer);
        }
        assertThat(totalClosed).isEqualTo(due.size());
        for (Auction auction : due) {
            assertThat(reload(auction.id()).status()).isEqualTo(AuctionStatus.CLOSED);
            assertThat(jdbc.sql("SELECT count(*) FROM auction_events WHERE auction_id = :id AND type = 'AUCTION_CLOSED'")
                    .param("id", auction.id()).query(Long.class).single()).isEqualTo(1);
            assertThat(reload(auction.id()).lastSeq()).isEqualTo(2);
        }
    }

    // ---- helpers ----------------------------------------------------------------------------

    private List<BidResult> stampede(String strategyName, Auction auction, int bidders,
                                     IntToLongFunction amount) throws Exception {
        BidStrategy strategy = strategy(strategyName);
        List<User> users = new ArrayList<>();
        for (int i = 0; i < bidders; i++) {
            users.add(newUser("bidder"));
        }
        CountDownLatch start = new CountDownLatch(1);
        List<Future<BidResult>> futures = new ArrayList<>();
        for (int i = 0; i < bidders; i++) {
            BidCommand command = bid(auction, users.get(i), amount.applyAsLong(i));
            futures.add(threads.submit(() -> {
                await(start);
                return strategy.place(command);
            }));
        }
        start.countDown();
        List<BidResult> results = new ArrayList<>();
        for (Future<BidResult> future : futures) {
            results.add(get(future));
        }
        return results;
    }

    private static void assertStillBlocked(Future<?> future) throws Exception {
        try {
            Object result = future.get(400, TimeUnit.MILLISECONDS);
            throw new AssertionError("Expected the call to be blocked on the row lock, but it returned " + result);
        } catch (TimeoutException expected) {
            // Still waiting, as it should be.
        }
    }

    private void bumpVersionOnAnotherConnection(long auctionId) {
        try (Connection other = dataSource.getConnection();
             PreparedStatement bump = other.prepareStatement("UPDATE auctions SET version = version + 1 WHERE id = ?")) {
            bump.setLong(1, auctionId);
            bump.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
