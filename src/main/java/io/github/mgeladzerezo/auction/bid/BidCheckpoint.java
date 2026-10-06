package io.github.mgeladzerezo.auction.bid;

/**
 * Named points inside a bid transaction where a test can pause a thread. Production uses
 * {@link #NONE}. The deterministic concurrency tests install latches here to force the exact
 * interleavings (two readers of the same version, a close between read and write, an
 * uncommitted bid) that a load test only hits by chance.
 */
public interface BidCheckpoint {

    BidCheckpoint NONE = new BidCheckpoint() {
    };

    /** After the auction row was read and the decision made, before anything is written. */
    default void afterRead(BidCommand command) {
    }

    /** After every write of an accepted bid, immediately before the transaction commits. */
    default void beforeCommit(BidCommand command) {
    }
}
