package io.github.mgeladzerezo.auction.realtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mgeladzerezo.auction.support.Api;
import io.github.mgeladzerezo.auction.support.TestApp;
import io.github.mgeladzerezo.auction.support.TestSocket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.JsonNode;

/**
 * Two complete application instances sharing one database, as behind a load balancer. Proves
 * that an event produced on one instance reaches subscribers on the other (LISTEN/NOTIFY), in
 * the same order with the same sequence numbers, and that two schedulers close an auction once.
 */
class TwoInstanceTest {

    private static TestApp instanceA;
    private static TestApp instanceB;

    @BeforeAll
    static void startBoth() {
        instanceA = TestApp.start("node-a");
        instanceB = TestApp.start("node-b");
    }

    @AfterAll
    static void stopBoth() {
        instanceB.close();
        instanceA.close();
    }

    @Test
    void bidAcceptedOnOneInstanceReachesSubscribersOfTheOtherInOrder() {
        Api apiA = new Api(instanceA.port());
        Api apiB = new Api(instanceB.port());
        JsonNode alice = apiA.register("alice_" + UUID.randomUUID().toString().substring(0, 8));
        JsonNode bob = apiB.register("bob_" + UUID.randomUUID().toString().substring(0, 8));

        // Created through A; both schedulers are running, exactly one of them opens it.
        long auctionId = apiA.post("/api/auctions", alice.path("token").asString(), """
                {"title":"Cross-instance lot","startPrice":1000,"minIncrement":100,"durationSeconds":4,
                 "antiSnipeWindowSeconds":0,"maxExtensions":0}
                """).body().path("id").asLong();

        JsonNode carol = apiA.register("carol_" + UUID.randomUUID().toString().substring(0, 8));
        try (TestSocket onA = TestSocket.connect(instanceA.port(), null);
             TestSocket onB = TestSocket.connect(instanceB.port(), null)) {
            assertThat(onA.next("WELCOME").path("instance").asString()).isEqualTo("node-a");
            assertThat(onB.next("WELCOME").path("instance").asString()).isEqualTo("node-b");
            onA.subscribe(auctionId);
            onB.subscribe(auctionId);

            // Bob bids through instance B and Carol through instance A, alternately, six bids in all.
            // The first attempts may arrive before a scheduler has opened the auction.
            long amount = 1000;
            int accepted = 0;
            for (int i = 0; i < 400 && accepted < 6; i++) {
                boolean bobsTurn = accepted % 2 == 0;
                JsonNode result = (bobsTurn ? apiB : apiA).post("/api/auctions/" + auctionId + "/bids",
                        (bobsTurn ? bob : carol).path("token").asString(),
                        "{\"amount\":" + amount + ",\"clientBidId\":\"two-" + i + "\"}").body();
                if (result.path("outcome").asString().equals("ACCEPTED")) {
                    accepted++;
                    amount += 100;
                } else {
                    assertThat(result.path("reason").asString()).isEqualTo("NOT_STARTED");
                    sleep(25);
                }
            }
            assertThat(accepted).isEqualTo(6);

            // Wait for the close on both sockets and compare the complete streams.
            List<JsonNode> seenOnA = eventsUntilSettled(onA);
            List<JsonNode> seenOnB = eventsUntilSettled(onB);

            assertThat(seenOnA.stream().map(e -> e.path("seq").asLong()))
                    .containsExactlyElementsOf(seenOnB.stream().map(e -> e.path("seq").asLong()).toList());
            assertThat(seenOnA).isEqualTo(seenOnB);
            List<String> types = seenOnA.stream().map(e -> e.path("type").asString()).toList();
            assertThat(types).containsSubsequence("BID_ACCEPTED", "BID_ACCEPTED", "BID_ACCEPTED", "BID_ACCEPTED",
                    "BID_ACCEPTED", "BID_ACCEPTED", "AUCTION_CLOSED", "AUCTION_SETTLED");
            long firstSeq = seenOnA.getFirst().path("seq").asLong();
            for (int i = 0; i < seenOnA.size(); i++) {
                assertThat(seenOnA.get(i).path("seq").asLong()).as("no gaps").isEqualTo(firstSeq + i);
            }
        }

        // Two schedulers raced for it: exactly one open, one close, one settlement.
        JdbcClient jdbc = instanceA.bean(JdbcClient.class);
        for (String type : List.of("AUCTION_OPENED", "AUCTION_CLOSED", "AUCTION_SETTLED")) {
            assertThat(jdbc.sql("SELECT count(*) FROM auction_events WHERE auction_id = :id AND type = :type")
                    .param("id", auctionId).param("type", type).query(Long.class).single()).as(type).isEqualTo(1);
        }
        assertThat(apiB.get("/api/auctions/" + auctionId + "/audit", null).body().path("consistent").asBoolean()).isTrue();
    }

    /** Collects the auction events a socket received, up to and including AUCTION_SETTLED. */
    private static List<JsonNode> eventsUntilSettled(TestSocket socket) {
        List<JsonNode> events = new ArrayList<>();
        Instant deadline = Instant.now().plusSeconds(30);
        while (Instant.now().isBefore(deadline)) {
            JsonNode message = socket.next();
            if (message.has("seq") && !message.path("type").asString().equals("SNAPSHOT")) {
                events.add(message);
                if (message.path("type").asString().equals("AUCTION_SETTLED")) {
                    return events;
                }
            }
        }
        throw new AssertionError("AUCTION_SETTLED not seen; got " + events);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
