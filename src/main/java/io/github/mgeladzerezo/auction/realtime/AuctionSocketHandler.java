package io.github.mgeladzerezo.auction.realtime;

import io.github.mgeladzerezo.auction.auth.User;
import io.github.mgeladzerezo.auction.bid.BidCommand;
import io.github.mgeladzerezo.auction.bid.BidResult;
import io.github.mgeladzerezo.auction.bid.BidService;
import io.github.mgeladzerezo.auction.config.AuctionProperties;
import io.github.mgeladzerezo.auction.config.DbClock;
import io.github.mgeladzerezo.auction.metrics.AuctionMetrics;
import jakarta.websocket.Session;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.adapter.NativeWebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The WebSocket endpoint: a small JSON protocol over raw text frames.
 *
 * <p>Client to server: {@code SUBSCRIBE {auctionId, lastSeq?}}, {@code UNSUBSCRIBE {auctionId}},
 * {@code BID {auctionId, amount, clientBidId}}, {@code PING {clientTime?}}.
 * Server to client: {@code WELCOME}, {@code SNAPSHOT}, {@code REPLAY}, the auction events,
 * {@code BID_RESULT}, {@code PONG}, {@code ERROR}.
 *
 * <p>Messages of one session are handled one at a time, on a virtual thread, and a bid is
 * processed inline. A session therefore cannot have more than one bid in flight on the server,
 * which is free per-client backpressure: a client that floods bids only delays itself.
 */
@Component
public class AuctionSocketHandler extends TextWebSocketHandler {

    /** Handshake attribute under which {@link TokenHandshakeInterceptor} stores the user. */
    static final String USER_ATTRIBUTE = "auction.user";

    private static final Logger log = LoggerFactory.getLogger(AuctionSocketHandler.class);
    private static final String TOMCAT_SEND_TIMEOUT = "org.apache.tomcat.websocket.BLOCKING_SEND_TIMEOUT";

    private final Map<String, ClientConnection> connections = new ConcurrentHashMap<>();
    private final AuctionHub hub;
    private final BidService bids;
    private final JsonMapper json;
    private final DbClock clock;
    private final AuctionMetrics metrics;
    private final AuctionProperties properties;

    public AuctionSocketHandler(AuctionHub hub, BidService bids, JsonMapper json, DbClock clock,
                                AuctionMetrics metrics, AuctionProperties properties) {
        this.hub = hub;
        this.bids = bids;
        this.json = json;
        this.clock = clock;
        this.metrics = metrics;
        this.properties = properties;
        metrics.gauge("auction.ws.sessions", "Open WebSocket sessions on this instance", connections::size);
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        // Bound how long one write may block the session's writer thread on a stalled socket.
        if (session instanceof NativeWebSocketSession nativeSession
                && nativeSession.getNativeSession() instanceof Session jakartaSession) {
            jakartaSession.getUserProperties().put(TOMCAT_SEND_TIMEOUT, properties.realtime().sendTimeout().toMillis());
        }
        User user = (User) session.getAttributes().get(USER_ATTRIBUTE);
        ClientConnection connection = new ClientConnection(session.getId(), user, new SessionTransport(session),
                properties.realtime().sendQueueCapacity(), metrics::slowConsumerDropped);
        connections.put(session.getId(), connection);
        send(connection, new ServerMessage.Welcome(session.getId(), properties.instanceId(),
                user == null ? null : user.id(), user == null ? null : user.username(), clock.now(),
                bids.strategyName()));
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        ClientConnection connection = connections.get(session.getId());
        if (connection == null) {
            return;
        }
        JsonNode request;
        try {
            request = json.readTree(message.getPayload());
        } catch (JacksonException e) {
            fail(connection, "INVALID_MESSAGE", "Message is not valid JSON", null, null, false);
            return;
        }
        String type = request.path("type").asString("");
        try {
            switch (type) {
                case "SUBSCRIBE" -> hub.subscribe(connection, requiredLong(request, "auctionId"),
                        request.hasNonNull("lastSeq") ? requiredLong(request, "lastSeq") : null);
                case "UNSUBSCRIBE" -> {
                    long auctionId = requiredLong(request, "auctionId");
                    hub.unsubscribe(connection, auctionId);
                    send(connection, new ServerMessage.Unsubscribed(auctionId));
                }
                case "BID" -> bid(connection, request);
                case "PING" -> send(connection, new ServerMessage.Pong(
                        request.hasNonNull("clientTime") ? request.get("clientTime").asLong() : null, clock.now()));
                default -> fail(connection, "UNKNOWN_TYPE", "Unknown message type '" + type + "'", null, null, false);
            }
        } catch (IllegalArgumentException e) {
            fail(connection, "INVALID_MESSAGE", e.getMessage(), null, null, false);
        } catch (RuntimeException e) {
            log.warn("Failed to handle {} on session {}: {}", type, session.getId(), e.toString());
            fail(connection, "UNAVAILABLE", "Temporarily unavailable", null, null, true);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        ClientConnection connection = connections.remove(session.getId());
        if (connection != null) {
            hub.disconnect(connection);
            connection.terminated();
        }
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.debug("Transport error on session {}: {}", session.getId(), exception.toString());
    }

    /** Number of open sessions on this instance. */
    public int sessionCount() {
        return connections.size();
    }

    private void bid(ClientConnection connection, JsonNode request) {
        String clientBidId = request.path("clientBidId").asString(null);
        Long auctionId = request.hasNonNull("auctionId") && request.get("auctionId").canConvertToLong()
                ? request.get("auctionId").longValue() : null;
        if (connection.user() == null) {
            fail(connection, "UNAUTHENTICATED", "Log in to bid", clientBidId, auctionId, false);
            return;
        }
        BidCommand command;
        try {
            command = new BidCommand(requiredLong(request, "auctionId"), connection.user().id(),
                    connection.user().username(), requiredLong(request, "amount"), clientBidId);
        } catch (IllegalArgumentException e) {
            fail(connection, "INVALID_MESSAGE", e.getMessage(), clientBidId, auctionId, false);
            return;
        }
        BidResult result;
        try {
            result = bids.place(command);
        } catch (RuntimeException e) {
            // The outcome is unknown here (the commit may or may not have happened). The answer
            // the client is owed is whatever the ledger says when it asks again with the same id.
            log.warn("Bid {} by user {} failed without an answer: {}", clientBidId, connection.user().id(), e.toString());
            fail(connection, "UNAVAILABLE", "No answer yet: re-send this bid with the same clientBidId",
                    clientBidId, auctionId, true);
            return;
        }
        send(connection, new ServerMessage.BidAnswer(result));
    }

    private static long requiredLong(JsonNode request, String field) {
        JsonNode value = request.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToLong()) {
            throw new IllegalArgumentException("'" + field + "' must be an integer");
        }
        return value.longValue();
    }

    private void fail(ClientConnection connection, String code, String message, String clientBidId, Long auctionId,
                      boolean retryable) {
        send(connection, new ServerMessage.Failure(code, message, clientBidId, auctionId, retryable));
    }

    private void send(ClientConnection connection, ServerMessage message) {
        connection.send(json.writeValueAsString(message));
    }

    /** Adapts a Spring session to the outbox's transport. Only the writer thread calls send. */
    private record SessionTransport(WebSocketSession session) implements ClientConnection.Transport {

        @Override
        public void send(String text) throws IOException {
            session.sendMessage(new TextMessage(text));
        }

        @Override
        public void close(int code, String reason) {
            try {
                session.close(new CloseStatus(code, reason));
            } catch (IOException | RuntimeException e) {
                log.debug("Closing session {} failed: {}", session.getId(), e.toString());
            }
        }
    }
}
