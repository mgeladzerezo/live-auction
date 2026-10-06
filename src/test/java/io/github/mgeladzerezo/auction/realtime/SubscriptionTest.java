package io.github.mgeladzerezo.auction.realtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.mgeladzerezo.auction.event.StoredEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The per-subscription state machine that joins a database snapshot to the live notification
 * stream without gaps or duplicates.
 */
class SubscriptionTest {

    private final List<String> sent = new CopyOnWriteArrayList<>();
    private final ClientConnection connection = new ClientConnection("c", null, new ClientConnection.Transport() {
        @Override
        public void send(String text) {
            sent.add(text);
        }

        @Override
        public void close(int code, String reason) {
            sent.add("CLOSED:" + code);
        }
    }, 1000, () -> { });
    private final Subscription subscription = new Subscription(connection, 42);

    @AfterEach
    void stopWriter() {
        connection.terminated();
    }

    private static StoredEvent event(long seq) {
        return new StoredEvent(42, seq, Instant.EPOCH, "e" + seq);
    }

    private void assertSent(String... expected) {
        await().atMost(Duration.ofSeconds(5)).until(() -> sent.size() >= expected.length);
        assertThat(sent).containsExactly(expected);
    }

    @Test
    void eventsArrivingDuringSyncAreHeldBackThenDeliveredAfterTheSnapshot() {
        // Registered, snapshot not read yet: 6 and 7 arrive live.
        assertThat(subscription.deliver(event(6))).isFalse();
        assertThat(subscription.deliver(event(7))).isFalse();
        assertThat(sent).isEmpty();

        // The snapshot turned out to be at seq 5.
        assertThat(subscription.completeSync(5, "snapshot@5")).isFalse();

        assertSent("snapshot@5", "e6", "e7");
        assertThat(subscription.deliveredSeq()).isEqualTo(7);
        assertThat(subscription.isLive()).isTrue();
    }

    @Test
    void bufferedEventsAlreadyCoveredByTheSnapshotAreNotSentTwice() {
        // In flight when we registered, but the snapshot (read later) already includes them.
        subscription.deliver(event(4));
        subscription.deliver(event(5));
        subscription.deliver(event(6));

        subscription.completeSync(5, "snapshot@5");

        assertSent("snapshot@5", "e6");
    }

    @Test
    void liveEventsAreDeliveredInOrderAndDuplicatesDropped() {
        subscription.completeSync(1, "snapshot@1");
        assertThat(subscription.deliver(event(2))).isFalse();
        assertThat(subscription.deliver(event(2))).isFalse();
        assertThat(subscription.deliver(event(1))).isFalse();
        assertThat(subscription.deliver(event(3))).isFalse();

        assertSent("snapshot@1", "e2", "e3");
    }

    @Test
    void skippedSequenceNumberIsReportedAndNothingIsSentUntilResynchronised() {
        subscription.completeSync(1, "snapshot@1");
        subscription.deliver(event(2));

        // 3 never arrives.
        assertThat(subscription.deliver(event(4))).isTrue();
        assertThat(subscription.isLive()).isFalse();
        assertThat(subscription.deliver(event(5))).isFalse();
        assertSent("snapshot@1", "e2");

        // The resync replays up to 4 from the log; 5 was buffered and follows on.
        assertThat(subscription.completeSync(4, "replay(2,4]")).isFalse();
        assertSent("snapshot@1", "e2", "replay(2,4]", "e5");
        assertThat(subscription.deliveredSeq()).isEqualTo(5);
    }

    @Test
    void syncThatDoesNotConnectToTheBufferAsksForAnotherRound() {
        subscription.deliver(event(9));

        // Snapshot at 5, buffer starts at 9: events 6..8 are missing.
        assertThat(subscription.completeSync(5, "snapshot@5")).isTrue();
        assertThat(subscription.isLive()).isFalse();
        assertThat(subscription.deliveredSeq()).isEqualTo(5);

        assertThat(subscription.completeSync(9, "replay(5,9]")).isFalse();
        assertSent("snapshot@5", "replay(5,9]");
        assertThat(subscription.isLive()).isTrue();
    }

    @Test
    void beginResyncIsGrantedToOneCallerOnly() {
        subscription.completeSync(3, "snapshot@3");

        assertThat(subscription.beginResync()).isTrue();
        assertThat(subscription.beginResync()).isFalse();
        assertThat(subscription.isLive()).isFalse();
    }
}
