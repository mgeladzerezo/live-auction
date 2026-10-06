package io.github.mgeladzerezo.auction.event;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Properties;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.jdbc.autoconfigure.JdbcConnectionDetails;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Holds one dedicated PostgreSQL connection in {@code LISTEN} mode and feeds every committed
 * event into the local {@link EventFanout} (the WebSocket hub).
 *
 * <p>This is how a bid accepted by instance A reaches a browser connected to instance B: both
 * instances listen on the same channel, and PostgreSQL delivers a transaction's notifications
 * to all listeners when, and only if, the transaction commits. For a single auction the
 * delivery order equals the commit order, which equals sequence-number order, because each
 * event's transaction held the auction's row lock.
 *
 * <p>The connection is not taken from the pool: it is held forever and would otherwise cost
 * the bidding path a pooled connection. If it breaks, the loop reconnects and tells the hub to
 * re-read its subscriptions from the event log, since notifications are not queued for a
 * listener that is not connected.
 */
@Component
public class NotificationListener implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(NotificationListener.class);
    private static final int POLL_MILLIS = 500;
    private static final long RECONNECT_DELAY_MILLIS = 1000;

    private final JdbcConnectionDetails database;
    private final EventFanout hub;
    private final JsonMapper json;
    private volatile boolean running;
    private volatile boolean listening;
    private Thread thread;

    public NotificationListener(JdbcConnectionDetails database, EventFanout hub, JsonMapper json) {
        this.database = database;
        this.hub = hub;
        this.json = json;
    }

    @Override
    public void start() {
        running = true;
        thread = Thread.ofPlatform().name("auction-events-listener").daemon().start(this::run);
    }

    /** Stops the loop; it notices within one poll interval and closes its connection itself. */
    @Override
    public void stop() {
        running = false;
        try {
            if (thread != null) {
                thread.join(4L * POLL_MILLIS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Whether the LISTEN connection is currently established. */
    public boolean isListening() {
        return listening;
    }

    private void run() {
        while (running) {
            try (Connection fresh = DriverManager.getConnection(database.getJdbcUrl(), connectionProperties())) {
                try (Statement statement = fresh.createStatement()) {
                    statement.execute("LISTEN " + EventStore.CHANNEL);
                }
                listening = true;
                log.info("Listening for auction events on channel '{}'", EventStore.CHANNEL);
                // Anything committed while we were not listening was never delivered to us.
                hub.resynchroniseAll();
                PGConnection postgres = fresh.unwrap(PGConnection.class);
                while (running) {
                    PGNotification[] notifications = postgres.getNotifications(POLL_MILLIS);
                    if (notifications != null) {
                        for (PGNotification notification : notifications) {
                            dispatch(notification.getParameter());
                        }
                    }
                }
            } catch (SQLException e) {
                if (running) {
                    log.warn("Event listener connection lost, reconnecting: {}", e.toString());
                }
            } finally {
                listening = false;
            }
            if (running) {
                try {
                    Thread.sleep(RECONNECT_DELAY_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void dispatch(String payload) {
        try {
            JsonNode node = json.readTree(payload);
            hub.dispatch(new StoredEvent(node.required("auctionId").longValue(), node.required("seq").longValue(),
                    Instant.parse(node.required("at").stringValue()), payload));
        } catch (RuntimeException e) {
            // A malformed payload must not kill the listener; the hub's sweep will notice the gap.
            log.error("Dropping undecodable event notification: {}", e.toString());
        }
    }

    private Properties connectionProperties() {
        Properties properties = new Properties();
        properties.setProperty("user", database.getUsername());
        properties.setProperty("password", database.getPassword());
        properties.setProperty("ApplicationName", "live-auction-listener");
        // The connection is idle between events; keep-alives let a dead peer be noticed.
        properties.setProperty("tcpKeepAlive", "true");
        return properties;
    }
}
