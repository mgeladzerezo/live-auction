package io.github.mgeladzerezo.auction;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mgeladzerezo.auction.auction.Auction;
import io.github.mgeladzerezo.auction.support.AbstractIntegrationTest;
import io.github.mgeladzerezo.auction.support.Api;
import io.github.mgeladzerezo.auction.support.TestSocket;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

/** The REST API over real HTTP: authentication, auction CRUD, bidding, audit and metrics. */
class ApiIntegrationTest extends AbstractIntegrationTest {

    private Api api;

    @BeforeEach
    void client() {
        api = new Api(port);
    }

    private static String unique(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().substring(0, 8);
    }

    @Test
    void registerLoginAndTokenLifecycle() {
        String username = unique("carol");
        JsonNode registered = api.register(username);
        String token = registered.path("token").asString();
        assertThat(token).hasSizeGreaterThan(30);
        assertThat(registered.path("username").asString()).isEqualTo(username);

        assertThat(api.get("/api/auth/me", token).body().path("username").asString()).isEqualTo(username);
        assertThat(api.get("/api/auth/me", null).status()).isEqualTo(401);
        assertThat(api.get("/api/auth/me", token + "x").status()).isEqualTo(401);

        // Usernames are unique regardless of case.
        Api.Response duplicate = api.post("/api/auth/register", null,
                "{\"username\":\"" + username.toUpperCase() + "\",\"password\":\"another-password\"}");
        assertThat(duplicate.status()).isEqualTo(409);
        assertThat(duplicate.body().path("code").asString()).isEqualTo("USERNAME_TAKEN");

        Api.Response login = api.post("/api/auth/login", null,
                "{\"username\":\"" + username + "\",\"password\":\"correct-horse-battery\"}");
        assertThat(login.status()).isEqualTo(200);
        assertThat(login.body().path("token").asString()).isNotEqualTo(token);

        for (String body : new String[] {
                "{\"username\":\"" + username + "\",\"password\":\"wrong-password\"}",
                "{\"username\":\"nobody-here\",\"password\":\"correct-horse-battery\"}"}) {
            Api.Response refused = api.post("/api/auth/login", null, body);
            assertThat(refused.status()).isEqualTo(401);
            assertThat(refused.body().path("code").asString()).isEqualTo("BAD_CREDENTIALS");
        }

        assertThat(api.post("/api/auth/register", null, "{\"username\":\"ab\",\"password\":\"long-enough-pw\"}").status())
                .isEqualTo(400);
        assertThat(api.post("/api/auth/register", null, "{\"username\":\"" + unique("x") + "\",\"password\":\"short\"}")
                .status()).isEqualTo(400);
    }

    @Test
    void passwordsAndTokensAreNotStoredInTheClear() {
        String username = unique("dave");
        String token = api.register(username).path("token").asString();

        String hash = jdbc.sql("SELECT password_hash FROM users WHERE username = :u").param("u", username)
                .query(String.class).single();
        assertThat(hash).startsWith("pbkdf2$").doesNotContain("correct-horse-battery");
        assertThat(jdbc.sql("SELECT count(*) FROM auth_tokens WHERE token_hash = :t").param("t", token)
                .query(Long.class).single()).isZero();
    }

    @Test
    void createListAndReadAuctions() {
        String token = api.register(unique("seller")).path("token").asString();
        String body = """
                {"title":"Walnut desk","description":"Solid, heavy","startPrice":25000,"minIncrement":500,
                 "reservePrice":40000,"durationSeconds":600,"antiSnipeWindowSeconds":20,"maxExtensions":4}
                """;

        assertThat(api.post("/api/auctions", null, body).status()).isEqualTo(401);
        Api.Response created = api.post("/api/auctions", token, body);
        assertThat(created.status()).isEqualTo(201);
        long id = created.body().path("id").asLong();
        assertThat(created.body().path("status").asString()).isEqualTo("SCHEDULED");
        assertThat(created.body().path("hasReserve").asBoolean()).isTrue();
        assertThat(created.body().has("reservePrice")).as("the reserve itself is never exposed").isFalse();

        lifecycle.openDue();
        JsonNode detail = api.get("/api/auctions/" + id, null).body();
        assertThat(detail.path("auction").path("status").asString()).isEqualTo("OPEN");
        assertThat(detail.path("auction").path("minimumNextBid").asLong()).isEqualTo(25000);
        assertThat(detail.path("auction").path("maxExtensions").asInt()).isEqualTo(4);
        assertThat(detail.has("serverTime")).isTrue();

        JsonNode list = api.get("/api/auctions", null).body();
        assertThat(list.path("auctions").valueStream().map(a -> a.path("id").asLong())).contains(id);
        assertThat(list.path("instance").asString()).isEqualTo("test");

        assertThat(api.get("/api/auctions/999999999", null).status()).isEqualTo(404);
        Api.Response invalid = api.post("/api/auctions", token,
                "{\"title\":\"\",\"startPrice\":100,\"minIncrement\":10,\"durationSeconds\":60}");
        assertThat(invalid.status()).isEqualTo(400);
        assertThat(invalid.body().path("code").asString()).isEqualTo("INVALID_AUCTION");
        assertThat(api.post("/api/auctions", token, "{not json").status()).isEqualTo(400);
    }

    @Test
    void bidOverRestIsDefinitiveIdempotentAndReachesWebSocketSubscribers() {
        Auction auction = open(auction());
        JsonNode alice = api.register(unique("alice"));
        JsonNode bob = api.register(unique("bob"));
        String path = "/api/auctions/" + auction.id() + "/bids";

        try (TestSocket watcher = TestSocket.connect(port, null)) {
            watcher.next("WELCOME");
            watcher.subscribe(auction.id());
            watcher.next("SNAPSHOT");

            assertThat(api.post(path, null, "{\"amount\":1000,\"clientBidId\":\"a-1\"}").status()).isEqualTo(401);

            Api.Response accepted = api.post(path, alice.path("token").asString(), "{\"amount\":1000,\"clientBidId\":\"a-1\"}");
            assertThat(accepted.status()).isEqualTo(200);
            assertThat(accepted.body().path("outcome").asString()).isEqualTo("ACCEPTED");
            assertThat(watcher.next("BID_ACCEPTED").path("price").asLong()).isEqualTo(1000);

            // The same request again, as after a client-side timeout.
            Api.Response retried = api.post(path, alice.path("token").asString(), "{\"amount\":1000,\"clientBidId\":\"a-1\"}");
            assertThat(retried.body().path("outcome").asString()).isEqualTo("ACCEPTED");
            assertThat(retried.body().path("duplicate").asBoolean()).isTrue();
            assertThat(retried.body().path("bidId").asLong()).isEqualTo(accepted.body().path("bidId").asLong());

            // A rejection is a 200 with a reason, not an HTTP error.
            Api.Response rejected = api.post(path, bob.path("token").asString(), "{\"amount\":1001,\"clientBidId\":\"b-1\"}");
            assertThat(rejected.status()).isEqualTo(200);
            assertThat(rejected.body().path("outcome").asString()).isEqualTo("REJECTED");
            assertThat(rejected.body().path("reason").asString()).isEqualTo("TOO_LOW");
            assertThat(rejected.body().path("minimumBid").asLong()).isEqualTo(1100);

            assertThat(api.post(path, bob.path("token").asString(), "{\"amount\":1100}").status())
                    .as("clientBidId is mandatory").isEqualTo(400);
        }

        JsonNode history = api.get(path, null).body();
        assertThat(history).hasSize(1);
        assertThat(history.get(0).path("bidderName").asString()).isEqualTo(alice.path("username").asString());

        JsonNode report = api.get("/api/auctions/" + auction.id() + "/audit", null).body();
        assertThat(report.path("consistent").asBoolean()).isTrue();
        assertThat(report.path("bids").asInt()).isEqualTo(1);
    }

    @Test
    void actuatorExposesHealthAndTheAuctionMeters() {
        Auction auction = open(auction());
        String token = api.register(unique("erin")).path("token").asString();
        api.post("/api/auctions/" + auction.id() + "/bids", token, "{\"amount\":1000,\"clientBidId\":\"m-1\"}");
        api.post("/api/auctions/" + auction.id() + "/bids", token, "{\"amount\":5000,\"clientBidId\":\"m-2\"}");

        JsonNode health = api.get("/actuator/health", null).body();
        assertThat(health.path("status").asString()).isEqualTo("UP");

        JsonNode accepted = api.get("/actuator/metrics/auction.bids?tag=outcome:ACCEPTED", null).body();
        assertThat(accepted.path("measurements").get(0).path("value").asDouble()).isGreaterThanOrEqualTo(1);
        JsonNode rejected = api.get("/actuator/metrics/auction.bids?tag=reason:ALREADY_LEADING", null).body();
        assertThat(rejected.path("measurements").get(0).path("value").asDouble()).isGreaterThanOrEqualTo(1);

        for (String meter : new String[] {"auction.bid.latency", "auction.ws.sessions", "auction.ws.subscriptions",
                "auction.broadcast.lag", "auction.ws.slow.consumers"}) {
            assertThat(api.get("/actuator/metrics/" + meter, null).status()).as(meter).isEqualTo(200);
        }
        assertThat(api.get("/actuator/prometheus", null).status()).isEqualTo(200);
    }
}
