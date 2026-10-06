package io.github.mgeladzerezo.auction.support;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * A WebSocket client for tests, built on the JDK's {@code java.net.http.WebSocket}. Every
 * received message is parsed and queued in arrival order; tests pull from the queue with a
 * timeout and so assert on exactly what a real client would have seen, in the order it saw it.
 */
public final class TestSocket implements WebSocket.Listener, AutoCloseable {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final BlockingQueue<JsonNode> inbox = new LinkedBlockingQueue<>();
    private final StringBuilder partial = new StringBuilder();
    private final CompletableFuture<Integer> closed = new CompletableFuture<>();
    private WebSocket socket;

    private TestSocket() {
    }

    /** Connects to {@code /ws}; a null token connects as a spectator. Waits for WELCOME. */
    public static TestSocket connect(int port, String token) {
        TestSocket client = new TestSocket();
        String query = token == null ? "" : "?token=" + token;
        client.socket = HTTP.newWebSocketBuilder()
                .connectTimeout(TIMEOUT)
                .buildAsync(URI.create("ws://localhost:" + port + "/ws" + query), client)
                .join();
        return client;
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        partial.append(data);
        if (last) {
            inbox.add(JSON.readTree(partial.toString()));
            partial.setLength(0);
        }
        webSocket.request(1);
        return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        closed.complete(statusCode);
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        closed.completeExceptionally(error);
    }

    public synchronized void send(String json) {
        socket.sendText(json, true).join();
    }

    public void subscribe(long auctionId) {
        send("{\"type\":\"SUBSCRIBE\",\"auctionId\":" + auctionId + "}");
    }

    public void subscribe(long auctionId, long lastSeq) {
        send("{\"type\":\"SUBSCRIBE\",\"auctionId\":" + auctionId + ",\"lastSeq\":" + lastSeq + "}");
    }

    public void bid(long auctionId, long amount, String clientBidId) {
        send("{\"type\":\"BID\",\"auctionId\":" + auctionId + ",\"amount\":" + amount
                + ",\"clientBidId\":\"" + clientBidId + "\"}");
    }

    /** The next message, whatever it is. Fails the test if none arrives in time. */
    public JsonNode next() {
        try {
            JsonNode message = inbox.poll(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (message == null) {
                throw new AssertionError("No WebSocket message within " + TIMEOUT);
            }
            return message;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for a message", e);
        }
    }

    /** The next message, asserted to have the given type. */
    public JsonNode next(String type) {
        JsonNode message = next();
        if (!type.equals(message.path("type").asString())) {
            throw new AssertionError("Expected " + type + " but received " + message);
        }
        return message;
    }

    /** Skips messages until one matches; fails if none does in time. */
    public JsonNode nextMatching(Predicate<JsonNode> wanted) {
        List<JsonNode> skipped = new ArrayList<>();
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                JsonNode message = inbox.poll(100, TimeUnit.MILLISECONDS);
                if (message != null) {
                    if (wanted.test(message)) {
                        return message;
                    }
                    skipped.add(message);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("No matching message within " + TIMEOUT + "; skipped " + skipped);
    }

    /** Skips messages until one of the given type arrives. */
    public JsonNode nextOfType(String type) {
        return nextMatching(message -> type.equals(message.path("type").asString()));
    }

    /** Returns a message if one arrives within the given time, otherwise null. */
    public JsonNode poll(Duration wait) {
        try {
            return inbox.poll(wait.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /** Waits for the server to close the connection and returns the close code. */
    public int awaitClose() {
        return closed.orTimeout(TIMEOUT.toSeconds(), TimeUnit.SECONDS).join();
    }

    @Override
    public void close() {
        if (!socket.isOutputClosed()) {
            socket.sendClose(WebSocket.NORMAL_CLOSURE, "done")
                    .orTimeout(2, TimeUnit.SECONDS)
                    .exceptionally(error -> null)
                    .join();
        }
        socket.abort();
    }
}
