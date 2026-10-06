package io.github.mgeladzerezo.auction.bid;

import java.time.Instant;

/**
 * An accepted bid as shown in the bid feed.
 *
 * @param seq      sequence number of the bid's BID_ACCEPTED event
 * @param extended whether this bid pushed the end time out
 */
public record BidView(long bidId, long seq, long bidderId, String bidderName, long amount, Instant at,
                      boolean extended) {
}
