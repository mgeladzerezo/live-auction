package io.github.mgeladzerezo.auction.realtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.mgeladzerezo.auction.auction.Auction;
import io.github.mgeladzerezo.auction.auth.AuthService;
import io.github.mgeladzerezo.auction.auth.User;
import io.github.mgeladzerezo.auction.bid.BidCheckpoint;
import io.github.mgeladzerezo.auction.bid.BidCommand;
import io.github.mgeladzerezo.auction.bid.BidService;
import io.github.mgeladzerezo.auction.support.AbstractIntegrationTest;
import io.github.mgeladzerezo.auction.support.TestSocket;
import java.net.http.WebSocketHandshakeException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import tools.jackson.databind.JsonNode;

/**
 * The WebSocket protocol against the running application: what a client receives, in which
 * order, with which sequence numbers, and how it recovers after missing something.
 */
class RealtimeIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    BidService bids;

    private static String id() {
        return UUID.randomUUID().toString();
    }

    @Test
    void welcomeIdentifiesTheUserAndCarriesServerTime() {
        AuthService.Session alice = register("alice");
        try (TestSocket socket = TestSocket.connect(port, alice.token())) {
            JsonNode welcome = socket.next("WELCOME");
            assertThat(welcome.path("userId").asLong()).isEqualTo(alice.user().id());
            assertThat(welcome.path("username").asString()).isEqualTo(alice.user().username());
            assertThat(welcome.path("instance").asString()).isEqualTo("test");
            assertThat(welcome.path("strategy").asString()).isEqualTo("optimistic");
            Instant serverTime = Instant.parse(welcome.path("serverTime").asString());
            assertThat(Duration.between(serverTime, dbNow()).abs()).isLessThan(Duration.ofSeconds(2));
        }
    }

    @Test
    void invalidTokenIsRefusedAtTheHandshake() {
        assertThatThrownBy(() -> TestSocket.connect(port, "not-a-real-token"))
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOfSatisfying(WebSocketHandshakeException.class,
                        refused -> assertThat(refused.getResponse().statusCode()).isEqualTo(401));
    }

    @Test
    void spectatorMaySubscribeButNotBid() {
        Auction auction = open(auction());
        try (TestSocket socket = TestSocket.connect(port, null)) {
            assertThat(socket.next("WELCOME").has("userId")).isFalse();
            socket.subscribe(auction.id());
            socket.next("SNAPSHOT");

            socket.bid(auction.id(), 1000, "spectator-bid");
            JsonNode error = socket.next("ERROR");
            assertThat(error.path("code").asString()).isEqualTo("UNAUTHENTICATED");
            assertThat(error.path("clientBidId").asString()).isEqualTo("spectator-bid");
            assertThat(count("bid_attempts", auction.id())).isZero();
        }
    }

    @Test
    void snapshotThenEventsWithConsecutiveSequenceNumbersForEverySubscriber() {
        Auction auction = open(auction().duration(Duration.ofSeconds(20)).antiSniping(30, 3));
        AuthService.Session alice = register("alice");
        AuthService.Session bob = register("bob");
        try (TestSocket a = TestSocket.connect(port, alice.token());
             TestSocket b = TestSocket.connect(port, bob.token());
             TestSocket watcher = TestSocket.connect(port, null)) {
            for (TestSocket socket : new TestSocket[] {a, b, watcher}) {
                socket.next("WELCOME");
                socket.subscribe(auction.id());
                JsonNode snapshot = socket.next("SNAPSHOT");
                assertThat(snapshot.path("seq").asLong()).isEqualTo(1);
                assertThat(snapshot.path("auction").path("status").asString()).isEqualTo("OPEN");
                assertThat(snapshot.path("auction").path("minimumNextBid").asLong()).isEqualTo(1000);
                assertThat(snapshot.path("auction").has("currentPrice")).isFalse();
                assertThat(snapshot.path("recentBids")).isEmpty();
            }

            // Alice bids inside the anti-sniping window: one bid event, one extension event.
            String aliceBid = id();
            a.bid(auction.id(), 1000, aliceBid);
            // Bob bids too low, then properly.
            String bobLow = id();
            String bobGood = id();

            for (TestSocket socket : new TestSocket[] {b, watcher}) {
                JsonNode accepted = socket.next("BID_ACCEPTED");
                assertThat(accepted.path("seq").asLong()).isEqualTo(2);
                assertThat(accepted.path("price").asLong()).isEqualTo(1000);
                assertThat(accepted.path("leaderId").asLong()).isEqualTo(alice.user().id());
                assertThat(accepted.path("leaderName").asString()).isEqualTo(alice.user().username());
                assertThat(accepted.path("minimumNextBid").asLong()).isEqualTo(1100);
                JsonNode extended = socket.next("TIME_EXTENDED");
                assertThat(extended.path("seq").asLong()).isEqualTo(3);
                assertThat(Instant.parse(extended.path("endsAt").asString())).isEqualTo(auction.endsAt().plusSeconds(30));
                assertThat(Instant.parse(extended.path("previousEndsAt").asString())).isEqualTo(auction.endsAt());
                assertThat(extended.path("extensionCount").asInt()).isEqualTo(1);
            }

            b.bid(auction.id(), 1050, bobLow);
            JsonNode rejected = b.next("BID_RESULT").path("result");
            assertThat(rejected.path("clientBidId").asString()).isEqualTo(bobLow);
            assertThat(rejected.path("outcome").asString()).isEqualTo("REJECTED");
            assertThat(rejected.path("reason").asString()).isEqualTo("TOO_LOW");
            assertThat(rejected.path("minimumBid").asLong()).isEqualTo(1100);

            b.bid(auction.id(), 1100, bobGood);
            JsonNode watcherSees = watcher.next("BID_ACCEPTED");
            assertThat(watcherSees.path("seq").asLong()).isEqualTo(4);
            assertThat(watcherSees.path("price").asLong()).isEqualTo(1100);
            assertThat(watcherSees.path("bidCount").asInt()).isEqualTo(2);

            // The bidder gets both its own answer and the public event; their relative order is not fixed.
            JsonNode aliceAnswer = a.nextMatching(m -> m.path("type").asString().equals("BID_RESULT")).path("result");
            assertThat(aliceAnswer.path("outcome").asString()).isEqualTo("ACCEPTED");
            assertThat(aliceAnswer.path("clientBidId").asString()).isEqualTo(aliceBid);
            assertThat(aliceAnswer.path("seq").asLong()).isEqualTo(2);
            assertThat(watcher.poll(Duration.ofMillis(300))).as("a rejected bid is not broadcast").isNull();
        }
    }

    @Test
    void snapshotOfARunningAuctionIncludesRecentBids() {
        Auction auction = open(auction());
        User alice = newUser("alice");
        User bob = newUser("bob");
        bids.place(bid(auction, alice, 1000));
        bids.place(bid(auction, bob, 1300));

        try (TestSocket socket = TestSocket.connect(port, null)) {
            socket.next("WELCOME");
            socket.subscribe(auction.id());
            JsonNode snapshot = socket.next("SNAPSHOT");

            assertThat(snapshot.path("seq").asLong()).isEqualTo(3);
            assertThat(snapshot.path("auction").path("currentPrice").asLong()).isEqualTo(1300);
            assertThat(snapshot.path("auction").path("leaderName").asString()).isEqualTo(bob.username());
            assertThat(snapshot.path("recentBids")).hasSize(2);
            assertThat(snapshot.path("recentBids").get(0).path("amount").asLong()).as("newest first").isEqualTo(1300);
            assertThat(snapshot.has("serverTime")).isTrue();
        }
    }

    @Test
    void reconnectWithLastSeqReplaysExactlyTheMissedEvents() {
        Auction auction = open(auction());
        User alice = newUser("alice");
        User bob = newUser("bob");
        long lastSeen;
        try (TestSocket socket = TestSocket.connect(port, null)) {
            socket.next("WELCOME");
            socket.subscribe(auction.id());
            socket.next("SNAPSHOT");
            bids.place(bid(auction, alice, 1000));
            lastSeen = socket.next("BID_ACCEPTED").path("seq").asLong();
        }

        // Three bids while the client is away.
        bids.place(bid(auction, bob, 1100));
        bids.place(bid(auction, alice, 1200));
        bids.place(bid(auction, bob, 1300));

        try (TestSocket socket = TestSocket.connect(port, null)) {
            socket.next("WELCOME");
            socket.subscribe(auction.id(), lastSeen);
            JsonNode replay = socket.next("REPLAY");

            assertThat(replay.path("fromSeq").asLong()).isEqualTo(lastSeen);
            assertThat(replay.path("seq").asLong()).isEqualTo(lastSeen + 3);
            assertThat(replay.path("events")).hasSize(3);
            for (int i = 0; i < 3; i++) {
                JsonNode event = replay.path("events").get(i);
                assertThat(event.path("type").asString()).isEqualTo("BID_ACCEPTED");
                assertThat(event.path("seq").asLong()).isEqualTo(lastSeen + 1 + i);
                assertThat(event.path("price").asLong()).isEqualTo(1100 + 100L * i);
            }

            // Live delivery continues right after the replay, without a gap or a repeat.
            bids.place(bid(auction, alice, 1400));
            assertThat(socket.next("BID_ACCEPTED").path("seq").asLong()).isEqualTo(lastSeen + 4);
        }
    }

    @Test
    void upToDateClientGetsAnEmptyReplayAndImpossibleLastSeqGetsASnapshot() {
        Auction auction = open(auction());
        try (TestSocket socket = TestSocket.connect(port, null)) {
            socket.next("WELCOME");
            socket.subscribe(auction.id(), auction.lastSeq());
            JsonNode replay = socket.next("REPLAY");
            assertThat(replay.path("events")).isEmpty();
            assertThat(replay.path("seq").asLong()).isEqualTo(auction.lastSeq());

            // Re-subscribing replaces the subscription; a lastSeq from the future cannot be replayed.
            socket.subscribe(auction.id(), 1_000_000);
            assertThat(socket.next("SNAPSHOT").path("seq").asLong()).isEqualTo(auction.lastSeq());

            socket.subscribe(999_999_999L);
            assertThat(socket.next("ERROR").path("code").asString()).isEqualTo("AUCTION_NOT_FOUND");
        }
    }

    @Test
    void nothingIsBroadcastBeforeTheBidTransactionCommits() throws Exception {
        Auction auction = open(auction());
        User alice = newUser("alice");
        BidCommand command = bid(auction, alice, 1000);
        CountDownLatch written = new CountDownLatch(1);
        CountDownLatch letCommit = new CountDownLatch(1);
        ledger.installCheckpoint(new BidCheckpoint() {
            @Override
            public void beforeCommit(BidCommand paused) {
                written.countDown();
                try {
                    letCommit.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        try (TestSocket socket = TestSocket.connect(port, null)) {
            socket.next("WELCOME");
            socket.subscribe(auction.id());
            socket.next("SNAPSHOT");

            Thread bidder = Thread.ofVirtual().start(() -> bids.place(command));
            assertThat(written.await(20, TimeUnit.SECONDS)).isTrue();

            // Bid row, auction row and event row are all written, but not committed.
            assertThat(socket.poll(Duration.ofMillis(500))).as("event leaked before commit").isNull();

            letCommit.countDown();
            assertThat(socket.next("BID_ACCEPTED").path("price").asLong()).isEqualTo(1000);
            bidder.join();
        }
    }

    @Test
    void duplicateSubmissionIsAnsweredFromTheLedgerAndBroadcastOnce() {
        Auction auction = open(auction());
        AuthService.Session alice = register("alice");
        try (TestSocket socket = TestSocket.connect(port, alice.token())) {
            socket.next("WELCOME");
            socket.subscribe(auction.id());
            socket.next("SNAPSHOT");
            String clientBidId = id();

            socket.bid(auction.id(), 1000, clientBidId);
            JsonNode first = socket.nextOfType("BID_RESULT").path("result");
            socket.bid(auction.id(), 1000, clientBidId);
            JsonNode second = socket.nextOfType("BID_RESULT").path("result");

            assertThat(first.path("outcome").asString()).isEqualTo("ACCEPTED");
            assertThat(first.path("duplicate").asBoolean()).isFalse();
            assertThat(second.path("outcome").asString()).isEqualTo("ACCEPTED");
            assertThat(second.path("duplicate").asBoolean()).isTrue();
            assertThat(second.path("bidId").asLong()).isEqualTo(first.path("bidId").asLong());
            assertThat(count("auction_events", auction.id())).as("opened + one bid").isEqualTo(2);
        }
    }

    @Test
    void closeAndSettlementAreAnnouncedWithTheWinner() throws Exception {
        Auction auction = open(auction().duration(Duration.ofMillis(1500)));
        User alice = newUser("alice");
        try (TestSocket socket = TestSocket.connect(port, null)) {
            socket.next("WELCOME");
            socket.subscribe(auction.id());
            socket.next("SNAPSHOT");
            bids.place(bid(auction, alice, 1000));
            socket.next("BID_ACCEPTED");

            awaitDbTime(auction.endsAt());
            lifecycle.tick();

            JsonNode closed = socket.next("AUCTION_CLOSED");
            assertThat(closed.path("seq").asLong()).isEqualTo(3);
            assertThat(closed.path("winnerId").asLong()).isEqualTo(alice.id());
            assertThat(closed.path("winnerName").asString()).isEqualTo(alice.username());
            assertThat(closed.path("finalPrice").asLong()).isEqualTo(1000);
            assertThat(closed.path("reserveMet").asBoolean()).isTrue();
            JsonNode settled = socket.next("AUCTION_SETTLED");
            assertThat(settled.path("seq").asLong()).isEqualTo(4);
            assertThat(settled.path("status").asString()).isEqualTo("SETTLED");
            assertThat(settled.path("winnerId").asLong()).isEqualTo(alice.id());
        }
    }

    /**
     * An event is committed but its notification never arrives (simulated by writing the event
     * row without NOTIFY). The next real event exposes the gap, and the server repairs it from
     * the event log before delivering anything further.
     */
    @Test
    void lostNotificationIsRepairedFromTheEventLogWhenTheNextEventExposesTheGap() {
        Auction auction = open(auction());
        User alice = newUser("alice");
        try (TestSocket socket = TestSocket.connect(port, null)) {
            socket.next("WELCOME");
            socket.subscribe(auction.id());
            socket.next("SNAPSHOT");

            commitEventWithoutNotifying(auction.id(), 2);
            bids.place(bid(auction, alice, 1000)); // becomes seq 3

            JsonNode replay = socket.next("REPLAY");
            assertThat(replay.path("fromSeq").asLong()).isEqualTo(1);
            assertThat(replay.path("events").get(0).path("seq").asLong()).isEqualTo(2);
            assertThat(replay.path("events").get(0).path("type").asString()).isEqualTo("SILENT");
            assertThat(replay.path("events").get(1).path("seq").asLong()).isEqualTo(3);
            assertThat(replay.path("events").get(1).path("type").asString()).isEqualTo("BID_ACCEPTED");
            assertThat(socket.poll(Duration.ofMillis(300))).as("seq 3 must not be delivered a second time").isNull();
        }
    }

    /** As above, but no further event follows: only the periodic sweep can notice. */
    @Test
    void lostNotificationWithNothingAfterItIsRepairedByTheSweep() {
        Auction auction = open(auction());
        try (TestSocket socket = TestSocket.connect(port, null)) {
            socket.next("WELCOME");
            socket.subscribe(auction.id());
            socket.next("SNAPSHOT");

            commitEventWithoutNotifying(auction.id(), 2);

            // The sweep runs every second here and acts on what the previous sweep saw.
            JsonNode replay = socket.next("REPLAY");
            assertThat(replay.path("events")).hasSize(1);
            assertThat(replay.path("events").get(0).path("seq").asLong()).isEqualTo(2);
        }
    }

    @Test
    void unsubscribeStopsEventsAndOneSocketCanFollowSeveralAuctions() {
        Auction first = open(auction());
        Auction second = open(auction());
        User alice = newUser("alice");
        try (TestSocket socket = TestSocket.connect(port, null)) {
            socket.next("WELCOME");
            socket.subscribe(first.id());
            socket.next("SNAPSHOT");
            socket.subscribe(second.id());
            socket.next("SNAPSHOT");

            bids.place(bid(first, alice, 1000));
            bids.place(bid(second, alice, 2000));
            assertThat(socket.next("BID_ACCEPTED").path("auctionId").asLong()).isEqualTo(first.id());
            assertThat(socket.next("BID_ACCEPTED").path("auctionId").asLong()).isEqualTo(second.id());

            socket.send("{\"type\":\"UNSUBSCRIBE\",\"auctionId\":" + first.id() + "}");
            socket.next("UNSUBSCRIBED");
            bids.place(bid(first, newUser("bob"), 1100));
            bids.place(bid(second, newUser("bob"), 2100));
            JsonNode event = socket.next("BID_ACCEPTED");
            assertThat(event.path("auctionId").asLong()).isEqualTo(second.id());
            assertThat(socket.poll(Duration.ofMillis(300))).isNull();
        }
    }

    @Test
    void pingIsAnsweredWithServerTimeAndBadMessagesWithErrors() {
        try (TestSocket socket = TestSocket.connect(port, register("alice").token())) {
            socket.next("WELCOME");

            socket.send("{\"type\":\"PING\",\"clientTime\":12345}");
            JsonNode pong = socket.next("PONG");
            assertThat(pong.path("clientTime").asLong()).isEqualTo(12345);
            assertThat(Duration.between(Instant.parse(pong.path("serverTime").asString()), dbNow()).abs())
                    .isLessThan(Duration.ofSeconds(2));

            socket.send("this is not json");
            assertThat(socket.next("ERROR").path("code").asString()).isEqualTo("INVALID_MESSAGE");
            socket.send("{\"type\":\"DANCE\"}");
            assertThat(socket.next("ERROR").path("code").asString()).isEqualTo("UNKNOWN_TYPE");
            socket.send("{\"type\":\"SUBSCRIBE\",\"auctionId\":\"seven\"}");
            assertThat(socket.next("ERROR").path("code").asString()).isEqualTo("INVALID_MESSAGE");
            socket.send("{\"type\":\"BID\",\"auctionId\":1,\"amount\":-5,\"clientBidId\":\"neg\"}");
            JsonNode badBid = socket.next("ERROR");
            assertThat(badBid.path("code").asString()).isEqualTo("INVALID_MESSAGE");
            assertThat(badBid.path("clientBidId").asString()).isEqualTo("neg");
            assertThat(badBid.path("retryable").asBoolean()).isFalse();
        }
    }

    /** Writes an event the way a bid would, minus the pg_notify. */
    private void commitEventWithoutNotifying(long auctionId, long seq) {
        jdbc.sql("""
                        WITH bumped AS (
                            UPDATE auctions SET last_seq = :seq, version = version + 1 WHERE id = :id RETURNING id)
                        INSERT INTO auction_events (auction_id, seq, type, payload, created_at)
                        SELECT id, :seq, 'SILENT',
                               jsonb_build_object('type', 'SILENT', 'auctionId', id, 'seq', :seq, 'at', clock_timestamp()),
                               clock_timestamp()
                          FROM bumped
                        """)
                .param("id", auctionId)
                .param("seq", seq)
                .update();
    }
}
