package io.github.mgeladzerezo.auction.load;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * One simulated bidder: a real WebSocket client (JDK {@code java.net.http.WebSocket}) that
 * follows one auction and keeps bidding until it is told the auction has closed.
 *
 * <p>It behaves like a careful real client, because that is what the "no bid lost" claim is
 * about: it checks event sequence numbers, re-sends an unanswered bid with the same
 * {@code clientBidId}, and after losing its connection reconnects with its {@code lastSeq}. It
 * also misbehaves on purpose at a configured rate, sending a bid twice or dropping the
 * connection with a bid in flight. Everything it sent and received is recorded for the
 * verifier; the bidder itself asserts nothing.
 */
final class StormBidder {

    /** One answer as received from the server. */
    record Answer(String outcome, String reason, long bidId, long amount, boolean duplicate) {

        /** The part of an answer that must be identical every time the same bid is answered. */
        String verdict() {
            return outcome + "/" + reason + "/" + bidId + "/" + amount;
        }
    }

    /** One bid attempt, identified by its client id, with everything that happened to it. */
    static final class Attempt {
        final String clientBidId = UUID.randomUUID().toString();
        final long amount;
        final List<Answer> answers = new ArrayList<>();
        int sends;
        /** True if the connection was dropped or the bid re-sent while this attempt was open. */
        boolean disturbed;
        long firstSentNanos;
        long firstAnswerNanos;
        private volatile CompletableFuture<Boolean> waiter = new CompletableFuture<>();

        Attempt(long amount) {
            this.amount = amount;
        }

        synchronized List<Answer> answersCopy() {
            return List.copyOf(answers);
        }
    }

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Duration ANSWER_TIMEOUT = Duration.ofSeconds(30);
    private static final int MAX_SENDS = 6;

    final int index;
    final long userId;
    final long auctionId;
    private final String token;
    private final URI endpoint;
    private final HttpClient http;
    private final BidStorm.Scenario scenario;
    private final Random random;
    private final CountDownLatch startGate;

    private final Object lock = new Object();
    private final Map<String, Attempt> attemptsById = new ConcurrentHashMap<>();
    final List<Attempt> attempts = new ArrayList<>();
    final List<String> problems = new ArrayList<>();

    // Event stream as this client saw it. Guarded by lock.
    private long[] fingerprints = new long[256];
    private int eventCount;
    long firstSeq = -1;
    long lastSeq = -1;
    int gaps;
    int extensionsSeen;
    int midRunSnapshots;
    JsonNode closedEvent;
    JsonNode settledEvent;
    private boolean open;
    private boolean closed;
    private long minimumNextBid;
    private long increment;
    private Long leaderId;

    int reconnects;
    int retryableErrors;
    int timeouts;
    int unanswered;

    private int generation;
    private volatile boolean connectionLost;
    private volatile WebSocket socket;
    private volatile Attempt inFlight;
    private volatile CompletableFuture<Void> synced = new CompletableFuture<>();
    final CountDownLatch settled = new CountDownLatch(1);

    StormBidder(int index, long userId, String token, long auctionId, URI endpoint, HttpClient http,
                BidStorm.Scenario scenario, CountDownLatch startGate) {
        this.index = index;
        this.userId = userId;
        this.token = token;
        this.auctionId = auctionId;
        this.endpoint = endpoint;
        this.http = http;
        this.scenario = scenario;
        this.random = new Random(scenario.seed() * 31 + index);
        this.startGate = startGate;
    }

    /** Opens the socket and subscribes; returns once the snapshot has arrived. */
    void connect() throws Exception {
        openSocket(null);
    }

    /** The bidding loop. Returns when the auction has closed and the last bid is answered. */
    void bidUntilClosed() throws Exception {
        startGate.await();
        while (true) {
            if (connectionLost) {
                reconnect(); // the server closed the socket (for example as a slow consumer)
            }
            boolean isOpen;
            synchronized (lock) {
                if (closed) {
                    break;
                }
                isOpen = open;
            }
            if (!isOpen) {
                Thread.sleep(10);
                continue;
            }
            Thread.sleep(scenario.minThinkMillis()
                    + random.nextInt(scenario.maxThinkMillis() - scenario.minThinkMillis() + 1));
            long amount;
            synchronized (lock) {
                if (closed) {
                    break;
                }
                // Mostly behave: do not outbid yourself. Sometimes do, to exercise ALREADY_LEADING.
                if (Objects.equals(leaderId, userId) && random.nextInt(10) != 0) {
                    continue;
                }
                amount = minimumNextBid + increment * random.nextInt(3);
            }
            Attempt attempt = new Attempt(amount);
            attempts.add(attempt);
            attemptsById.put(attempt.clientBidId, attempt);
            deliver(attempt);
        }
    }

    /** Waits for AUCTION_SETTLED, reconnecting if the connection is lost in the meantime. */
    boolean awaitSettled(Duration limit) throws Exception {
        long deadline = System.nanoTime() + limit.toNanos();
        while (!settled.await(100, TimeUnit.MILLISECONDS)) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            if (connectionLost) {
                reconnect();
            }
        }
        return true;
    }

    /** Whether every send made on an undisturbed connection has been answered. */
    boolean hasEveryOwedAnswer() {
        for (Attempt attempt : List.copyOf(attempts)) {
            if (!attempt.disturbed && attempt.answersCopy().size() < attempt.sends) {
                return false;
            }
        }
        return true;
    }

    void close() {
        WebSocket current = socket;
        if (current != null) {
            current.sendClose(WebSocket.NORMAL_CLOSURE, "done").exceptionally(error -> null);
        }
    }

    /** Fingerprints of the events this client received, in order, starting at {@link #firstSeq}. */
    long[] fingerprints() {
        synchronized (lock) {
            return Arrays.copyOf(fingerprints, eventCount);
        }
    }

    /**
     * Sends the bid and waits for its answer, re-sending with the same client id if the answer
     * does not come (dropped connection, retryable error, timeout).
     */
    private void deliver(Attempt attempt) throws Exception {
        boolean sendTwice = random.nextDouble() < scenario.duplicateRate();
        boolean dropWhileInFlight = random.nextDouble() < scenario.reconnectRate();
        for (int round = 1; round <= MAX_SENDS; round++) {
            attempt.waiter = new CompletableFuture<>();
            if (connectionLost) {
                attempt.disturbed |= round > 1;
                reconnect();
            }
            inFlight = attempt;
            try {
                send(attempt);
                if (sendTwice && round == 1) {
                    send(attempt);
                }
            } catch (RuntimeException e) {
                attempt.disturbed = true;
                reconnect();
                continue;
            }
            if (dropWhileInFlight && round == 1) {
                // The answer to this bid is now lost (or racing the abort). Only the retry can tell.
                attempt.disturbed = true;
                reconnect();
                continue;
            }
            try {
                if (attempt.waiter.get(ANSWER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    return;
                }
                attempt.disturbed = true; // retryable ERROR from the server
                Thread.sleep(50);
            } catch (TimeoutException e) {
                timeouts++;
                attempt.disturbed = true;
                reconnect();
            }
        }
        unanswered++;
    }

    private void send(Attempt attempt) {
        String message = "{\"type\":\"BID\",\"auctionId\":" + auctionId + ",\"amount\":" + attempt.amount
                + ",\"clientBidId\":\"" + attempt.clientBidId + "\"}";
        synchronized (this) {
            if (attempt.sends == 0) {
                attempt.firstSentNanos = System.nanoTime();
            }
            attempt.sends++;
            socket.sendText(message, true).join();
        }
    }

    /** Drops the connection without a goodbye and resumes from the last event seen. */
    private void reconnect() throws Exception {
        long resumeFrom;
        synchronized (lock) {
            generation++; // anything still arriving on the old socket is ignored from here on
            connectionLost = false;
            reconnects++;
            resumeFrom = lastSeq;
        }
        // Answers still owed on the old connection (a duplicate send, say) will never arrive now.
        for (Attempt attempt : attempts) {
            if (attempt.answersCopy().size() < attempt.sends) {
                attempt.disturbed = true;
            }
        }
        socket.abort();
        openSocket(resumeFrom);
    }

    private void openSocket(Long resumeFrom) throws Exception {
        int myGeneration;
        synchronized (lock) {
            myGeneration = generation;
        }
        CompletableFuture<Void> ready = new CompletableFuture<>();
        synced = ready;
        WebSocket connected = http.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .buildAsync(URI.create(endpoint + "?token=" + token), new Listener(myGeneration))
                .get(60, TimeUnit.SECONDS);
        socket = connected;
        String subscribe = "{\"type\":\"SUBSCRIBE\",\"auctionId\":" + auctionId
                + (resumeFrom == null ? "" : ",\"lastSeq\":" + resumeFrom) + "}";
        synchronized (this) {
            connected.sendText(subscribe, true).join();
        }
        ready.get(60, TimeUnit.SECONDS);
    }

    private void onMessage(int fromGeneration, String text) {
        JsonNode message = JSON.readTree(text);
        String type = message.path("type").asString();
        synchronized (lock) {
            if (fromGeneration != generation) {
                return;
            }
            switch (type) {
                case "WELCOME", "PONG", "UNSUBSCRIBED" -> {
                }
                case "SNAPSHOT" -> onSnapshot(message);
                case "REPLAY" -> {
                    message.path("events").forEach(this::onEvent);
                    synced.complete(null);
                }
                case "BID_RESULT" -> onAnswer(message.path("result"));
                case "ERROR" -> onError(message);
                default -> onEvent(message);
            }
        }
    }

    private void onSnapshot(JsonNode snapshot) {
        long seq = snapshot.path("seq").asLong();
        if (firstSeq < 0) {
            firstSeq = seq + 1;
        } else {
            // Continuity is lost: the events between lastSeq and seq were never delivered one by one.
            midRunSnapshots++;
        }
        lastSeq = seq;
        JsonNode auction = snapshot.path("auction");
        open = auction.path("status").asString().equals("OPEN");
        closed = !open && !auction.path("status").asString().equals("SCHEDULED");
        minimumNextBid = auction.path("minimumNextBid").asLong();
        increment = auction.path("minIncrement").asLong();
        leaderId = auction.hasNonNull("leaderId") ? auction.path("leaderId").asLong() : null;
        synced.complete(null);
    }

    private void onEvent(JsonNode event) {
        long seq = event.path("seq").asLong();
        if (seq <= lastSeq) {
            problems.add("received seq " + seq + " again after " + lastSeq);
            return;
        }
        if (seq != lastSeq + 1) {
            gaps++;
        }
        lastSeq = seq;
        if (eventCount == fingerprints.length) {
            fingerprints = Arrays.copyOf(fingerprints, eventCount * 2);
        }
        fingerprints[eventCount++] = fingerprint(event);
        switch (event.path("type").asString()) {
            case "AUCTION_OPENED" -> open = true;
            case "BID_ACCEPTED" -> {
                minimumNextBid = event.path("minimumNextBid").asLong();
                leaderId = event.path("leaderId").asLong();
            }
            case "TIME_EXTENDED" -> extensionsSeen++;
            case "AUCTION_CLOSED" -> {
                closed = true;
                open = false;
                closedEvent = event;
            }
            case "AUCTION_SETTLED" -> {
                settledEvent = event;
                settled.countDown();
            }
            default -> problems.add("unknown event type " + event.path("type"));
        }
    }

    private void onAnswer(JsonNode result) {
        Attempt attempt = attemptsById.get(result.path("clientBidId").asString());
        if (attempt == null) {
            problems.add("answer for a bid this client never sent: " + result);
            return;
        }
        Answer answer = new Answer(result.path("outcome").asString(), result.path("reason").asString("-"),
                result.path("bidId").asLong(0), result.path("amount").asLong(), result.path("duplicate").asBoolean());
        synchronized (attempt) {
            if (attempt.answers.isEmpty()) {
                attempt.firstAnswerNanos = System.nanoTime();
            }
            attempt.answers.add(answer);
        }
        attempt.waiter.complete(true);
    }

    private void onError(JsonNode error) {
        Attempt attempt = error.hasNonNull("clientBidId")
                ? attemptsById.get(error.path("clientBidId").asString()) : null;
        if (attempt != null && error.path("retryable").asBoolean()) {
            retryableErrors++;
            attempt.waiter.complete(false);
        } else {
            problems.add("unexpected ERROR: " + error);
        }
    }

    /**
     * A 64-bit digest of an event's content, independent of JSON key order, so the stream a
     * client received can be compared with the event log and with every other client cheaply.
     */
    static long fingerprint(JsonNode event) {
        Map<String, String> fields = new TreeMap<>();
        event.properties().forEach(entry -> fields.put(entry.getKey(), entry.getValue().asString()));
        long hash = 0xcbf29ce484222325L;
        for (Map.Entry<String, String> field : fields.entrySet()) {
            String pair = field.getKey() + "=" + field.getValue() + ";";
            for (int i = 0; i < pair.length(); i++) {
                hash ^= pair.charAt(i);
                hash *= 0x100000001b3L;
            }
        }
        return hash;
    }

    /** Receives frames for one connection; frames from a superseded connection are dropped. */
    private final class Listener implements WebSocket.Listener {

        private final int listenerGeneration;
        private final StringBuilder partial = new StringBuilder();

        Listener(int listenerGeneration) {
            this.listenerGeneration = listenerGeneration;
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            partial.append(data);
            if (last) {
                String text = partial.toString();
                partial.setLength(0);
                try {
                    onMessage(listenerGeneration, text);
                } catch (RuntimeException e) {
                    synchronized (lock) {
                        problems.add("could not process message: " + e);
                    }
                }
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            lost();
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            lost();
        }

        /** Flags the loss and wakes a bid that is waiting for an answer that will not come. */
        private void lost() {
            synchronized (lock) {
                if (listenerGeneration != generation) {
                    return;
                }
                connectionLost = true;
            }
            Attempt waiting = inFlight;
            if (waiting != null) {
                waiting.waiter.complete(false);
            }
        }
    }
}
