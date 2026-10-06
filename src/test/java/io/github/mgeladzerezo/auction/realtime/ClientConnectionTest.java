package io.github.mgeladzerezo.auction.realtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The bounded outbox: a consumer that stops reading must cost its producers nothing and must
 * end up disconnected, not silently missing messages.
 */
class ClientConnectionTest {

    /** A socket whose writes block until the test lets them through. */
    private static final class StalledSocket implements ClientConnection.Transport {

        final CountDownLatch unblock = new CountDownLatch(1);
        final CountDownLatch firstWriteStarted = new CountDownLatch(1);
        final List<String> written = new CopyOnWriteArrayList<>();
        final List<Integer> closeCodes = new CopyOnWriteArrayList<>();

        @Override
        public void send(String text) {
            firstWriteStarted.countDown();
            try {
                unblock.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            written.add(text);
        }

        @Override
        public void close(int code, String reason) {
            closeCodes.add(code);
        }
    }

    @Test
    void deliversInOrderWhenTheConsumerKeepsUp() {
        List<String> written = new CopyOnWriteArrayList<>();
        ClientConnection connection = new ClientConnection("c1", null, new ClientConnection.Transport() {
            @Override
            public void send(String text) {
                written.add(text);
            }

            @Override
            public void close(int code, String reason) {
            }
        }, 500, () -> { });

        for (int i = 0; i < 500; i++) {
            assertThat(connection.send("m" + i)).isTrue();
        }

        await().atMost(Duration.ofSeconds(5)).until(() -> written.size() == 500);
        for (int i = 0; i < 500; i++) {
            assertThat(written.get(i)).isEqualTo("m" + i);
        }
        connection.terminated();
    }

    @Test
    void stalledConsumerNeverBlocksTheProducerAndIsDisconnected() throws Exception {
        StalledSocket socket = new StalledSocket();
        AtomicInteger slowConsumerCallbacks = new AtomicInteger();
        int capacity = 16;
        ClientConnection connection = new ClientConnection("slow", null, socket, capacity,
                slowConsumerCallbacks::incrementAndGet);

        // The writer takes the first message and blocks inside the socket write.
        assertThat(connection.send("first")).isTrue();
        assertThat(socket.firstWriteStarted.await(5, TimeUnit.SECONDS)).isTrue();

        // Fill the queue, then overflow it. Every call must return promptly.
        long started = System.nanoTime();
        int accepted = 0;
        for (int i = 0; i < capacity; i++) {
            accepted += connection.send("queued-" + i) ? 1 : 0;
        }
        boolean overflowAccepted = connection.send("one too many");
        Duration producerTime = Duration.ofNanos(System.nanoTime() - started);

        assertThat(accepted).isEqualTo(capacity);
        assertThat(overflowAccepted).isFalse();
        assertThat(producerTime).isLessThan(Duration.ofMillis(500));
        assertThat(connection.isClosed()).isTrue();
        assertThat(connection.send("after close")).isFalse();
        await().atMost(Duration.ofSeconds(5)).until(() -> socket.closeCodes.contains(ClientConnection.SLOW_CONSUMER));
        assertThat(slowConsumerCallbacks).hasValue(1);

        // Nothing queued behind the stall is written after the disconnect.
        socket.unblock.countDown();
        Thread.sleep(100);
        assertThat(socket.written).doesNotContain("queued-0", "one too many", "after close");
        assertThat(socket.closeCodes).containsExactly(ClientConnection.SLOW_CONSUMER);
    }

    @Test
    void explicitCloseHappensOnceAndStopsDelivery() {
        StalledSocket socket = new StalledSocket();
        socket.unblock.countDown();
        ClientConnection connection = new ClientConnection("c2", null, socket, 4, () -> { });

        connection.close(1011, "bye");
        connection.close(1011, "bye again");

        await().atMost(Duration.ofSeconds(5)).until(() -> socket.closeCodes.size() == 1);
        assertThat(connection.send("late")).isFalse();
        assertThat(socket.closeCodes).containsExactly(1011);
    }
}
