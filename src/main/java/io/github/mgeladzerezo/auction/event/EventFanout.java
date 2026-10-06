package io.github.mgeladzerezo.auction.event;

/**
 * Where committed events go once the listener has received them. Implemented by the WebSocket
 * hub; kept as an interface so the event package does not depend on the transport.
 */
public interface EventFanout {

    /** Delivers one committed event to whoever follows its auction on this instance. */
    void dispatch(StoredEvent event);

    /**
     * Called after the notification stream was interrupted and has been re-established: events
     * may have been missed, so every consumer must be brought up to date from the event log.
     */
    void resynchroniseAll();
}
