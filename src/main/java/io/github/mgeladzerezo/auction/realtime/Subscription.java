package io.github.mgeladzerezo.auction.realtime;

import io.github.mgeladzerezo.auction.event.StoredEvent;
import java.util.ArrayList;
import java.util.List;

/**
 * One connection following one auction: the state machine that turns "a snapshot from the
 * database plus a stream of notifications" into a gap-free, duplicate-free sequence.
 *
 * <p>A subscription starts in <em>syncing</em> state. Live events that arrive meanwhile are
 * buffered, not sent. {@link #completeSync} then sends the snapshot (or replay) taken at
 * sequence S, sends the buffered events above S, and switches to <em>live</em>. Because the
 * subscription was registered with the hub <em>before</em> the snapshot was read, every event
 * is either inside the snapshot (seq &le; S) or in the buffer or still to come: nothing can
 * fall between the two.
 *
 * <p>All methods are synchronized on the subscription and only ever enqueue to the
 * connection's outbox, so the lock is held for nanoseconds.
 */
final class Subscription {

    /** Upper bound on events held while a synchronisation is in flight. */
    private static final int MAX_BUFFERED = 10_000;

    private final ClientConnection connection;
    private final long auctionId;
    private final List<StoredEvent> buffered = new ArrayList<>();
    private boolean syncing = true;
    private long deliveredSeq;

    Subscription(ClientConnection connection, long auctionId) {
        this.connection = connection;
        this.auctionId = auctionId;
    }

    ClientConnection connection() {
        return connection;
    }

    long auctionId() {
        return auctionId;
    }

    synchronized long deliveredSeq() {
        return deliveredSeq;
    }

    synchronized boolean isLive() {
        return !syncing;
    }

    /**
     * Offers a live event.
     *
     * @return true if the event revealed a gap (its predecessor never arrived); the subscription
     *         has then switched back to syncing and the caller must resynchronise it
     */
    synchronized boolean deliver(StoredEvent event) {
        if (syncing) {
            if (buffered.size() >= MAX_BUFFERED) {
                // A synchronisation that takes this long is not going to finish usefully.
                connection.close(ClientConnection.SLOW_CONSUMER, "synchronisation stalled: reconnect");
                buffered.clear();
            } else {
                buffered.add(event);
            }
            return false;
        }
        if (event.seq() <= deliveredSeq) {
            return false;
        }
        if (event.seq() != deliveredSeq + 1) {
            syncing = true;
            buffered.add(event);
            return true;
        }
        connection.send(event.json());
        deliveredSeq = event.seq();
        return false;
    }

    /**
     * Puts a live subscription back into syncing state ahead of a resynchronisation.
     *
     * @return false if it was already syncing (someone else is taking care of it)
     */
    synchronized boolean beginResync() {
        if (syncing) {
            return false;
        }
        syncing = true;
        return true;
    }

    /**
     * Sends the synchronisation message, then the buffered events that follow it, and goes live.
     *
     * @param syncMessage SNAPSHOT or REPLAY message that brings the client to {@code syncedSeq}
     * @return true if the buffer did not connect to {@code syncedSeq} without a gap; the
     *         subscription is then still syncing and must be synchronised again
     */
    synchronized boolean completeSync(long syncedSeq, String syncMessage) {
        connection.send(syncMessage);
        deliveredSeq = Math.max(deliveredSeq, syncedSeq);
        for (StoredEvent event : buffered) {
            if (event.seq() <= deliveredSeq) {
                continue;
            }
            if (event.seq() != deliveredSeq + 1) {
                buffered.clear();
                return true;
            }
            connection.send(event.json());
            deliveredSeq = event.seq();
        }
        buffered.clear();
        syncing = false;
        return false;
    }
}
