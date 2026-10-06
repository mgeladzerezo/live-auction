package io.github.mgeladzerezo.auction.event;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Reports the instance as unhealthy while its LISTEN connection is down. Such an instance still
 * accepts bids correctly, but its subscribers see nothing until the connection is back, so a
 * load balancer should prefer the others.
 */
@Component("eventListener")
public class ListenerHealthIndicator implements HealthIndicator {

    private final NotificationListener listener;

    public ListenerHealthIndicator(NotificationListener listener) {
        this.listener = listener;
    }

    @Override
    public Health health() {
        return listener.isListening()
                ? Health.up().withDetail("channel", EventStore.CHANNEL).build()
                : Health.down().withDetail("reason", "not listening for auction events").build();
    }
}
