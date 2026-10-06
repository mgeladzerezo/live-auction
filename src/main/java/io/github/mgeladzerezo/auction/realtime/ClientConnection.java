package io.github.mgeladzerezo.auction.realtime;

import io.github.mgeladzerezo.auction.auth.User;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One client socket with a bounded outbox.
 *
 * <p>Nothing that produces messages ever writes to a socket. Producers (the bid path answering
 * a bidder, the fan-out thread delivering an event to hundreds of subscribers) call
 * {@link #send}, which is a non-blocking {@code offer} into a bounded queue. A dedicated
 * virtual thread per connection takes messages off the queue and performs the blocking write.
 * A client that reads slowly therefore delays only its own writer thread.
 *
 * <p>When the queue is full the connection is closed with {@link #SLOW_CONSUMER}. Dropping
 * individual messages instead would leave a hole in the event sequence that the client would
 * have to detect and repair anyway; closing makes the client do the one thing that is always
 * right, which is to reconnect with its {@code lastSeq} and be replayed what it missed.
 */
public final class ClientConnection {

    /** The socket underneath, reduced to what the outbox needs. Lets tests supply a slow one. */
    public interface Transport {

        /** Writes one text message, blocking until the socket has taken it. */
        void send(String text) throws IOException;

        /** Closes the socket. May block; never called on a producer thread. */
        void close(int code, String reason);
    }

    /** WebSocket close code (application range) sent to a client that cannot keep up. */
    public static final int SLOW_CONSUMER = 4008;

    private static final Logger log = LoggerFactory.getLogger(ClientConnection.class);

    private final String id;
    private final User user;
    private final Transport transport;
    private final BlockingQueue<String> outbox;
    private final Runnable onSlowConsumer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Map<Long, Subscription> subscriptions = new ConcurrentHashMap<>();
    private final Thread writer;

    /**
     * @param user           the authenticated user, or {@code null} for a spectator
     * @param capacity       messages that may wait in the outbox
     * @param onSlowConsumer invoked once if the connection is dropped for overflowing
     */
    public ClientConnection(String id, User user, Transport transport, int capacity, Runnable onSlowConsumer) {
        this.id = id;
        this.user = user;
        this.transport = transport;
        this.outbox = new ArrayBlockingQueue<>(capacity);
        this.onSlowConsumer = onSlowConsumer;
        this.writer = Thread.ofVirtual().name("ws-writer-" + id).start(this::writeLoop);
    }

    public String id() {
        return id;
    }

    public User user() {
        return user;
    }

    /** Auctions this connection follows, keyed by auction id. Owned by {@link AuctionHub}. */
    Map<Long, Subscription> subscriptions() {
        return subscriptions;
    }

    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Queues a message for delivery. Never blocks.
     *
     * @return false if the connection is closed or was just closed for being too slow
     */
    public boolean send(String text) {
        if (closed.get()) {
            return false;
        }
        if (outbox.offer(text)) {
            return true;
        }
        if (closed.compareAndSet(false, true)) {
            log.debug("Connection {} overflowed its outbox of {} messages; closing", id, outbox.size());
            onSlowConsumer.run();
            shutdown(SLOW_CONSUMER, "slow consumer: reconnect with lastSeq");
        }
        return false;
    }

    /** Closes the socket from the server side with the given status. */
    public void close(int code, String reason) {
        if (closed.compareAndSet(false, true)) {
            shutdown(code, reason);
        }
    }

    /** Stops the writer after the socket has already gone away. */
    public void terminated() {
        closed.set(true);
        outbox.clear();
        writer.interrupt();
    }

    private void shutdown(int code, String reason) {
        outbox.clear();
        writer.interrupt();
        // Closing can block behind a write that is stuck on the slow socket, so it gets its
        // own thread; the caller may be the fan-out thread.
        Thread.ofVirtual().name("ws-close-" + id).start(() -> transport.close(code, reason));
    }

    private void writeLoop() {
        try {
            while (!closed.get()) {
                transport.send(outbox.take());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException | RuntimeException e) {
            if (closed.compareAndSet(false, true)) {
                log.debug("Write to connection {} failed: {}", id, e.toString());
                outbox.clear();
                Thread.ofVirtual().start(() -> transport.close(1011, "write failed"));
            }
        }
    }
}
