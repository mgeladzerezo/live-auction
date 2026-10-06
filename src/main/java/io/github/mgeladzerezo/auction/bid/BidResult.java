package io.github.mgeladzerezo.auction.bid;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.time.Instant;

/**
 * The single definitive answer to a bid attempt. Serialised with an {@code outcome} property of
 * {@code ACCEPTED} or {@code REJECTED}.
 *
 * <p>{@code duplicate} is true when the answer was not computed now but read back from the
 * attempt ledger because the same {@code clientBidId} had been answered before.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "outcome")
@JsonSubTypes({
        @JsonSubTypes.Type(value = BidResult.Accepted.class, name = "ACCEPTED"),
        @JsonSubTypes.Type(value = BidResult.Rejected.class, name = "REJECTED")
})
public sealed interface BidResult {

    String clientBidId();

    long auctionId();

    long amount();

    boolean duplicate();

    /**
     * @param seq        sequence number of the BID_ACCEPTED event for this bid
     * @param acceptedAt database clock reading the decision was made against
     * @param extendedTo new end time if this bid triggered an anti-sniping extension
     */
    record Accepted(String clientBidId, long auctionId, long amount, long bidId, long seq,
                    Instant acceptedAt, Instant extendedTo, boolean duplicate) implements BidResult {
    }

    /**
     * @param currentPrice price at decision time, when known
     * @param minimumBid   smallest amount that would have been accepted at decision time, when known
     */
    record Rejected(String clientBidId, long auctionId, long amount, RejectReason reason,
                    Long currentPrice, Long minimumBid, boolean duplicate) implements BidResult {
    }
}
