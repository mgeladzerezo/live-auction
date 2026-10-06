package io.github.mgeladzerezo.auction.bid;

import io.github.mgeladzerezo.auction.auction.Auction;
import io.github.mgeladzerezo.auction.auction.AuctionStatus;
import java.time.Instant;

/**
 * The part of an auction row a bid decision depends on, together with the database clock
 * reading taken by the same statement that read the row.
 *
 * @param version optimistic-lock token the write must still find in place
 * @param lastSeq last event sequence number already used by the auction
 * @param dbNow   {@code clock_timestamp()} at read time; the one clock every deadline uses
 */
public record BidState(
        long auctionId,
        AuctionStatus status,
        Long sellerId,
        long startPrice,
        long minIncrement,
        Long currentPrice,
        Long leaderId,
        Instant endsAt,
        int antiSnipeWindowSeconds,
        int maxExtensions,
        int extensionCount,
        int bidCount,
        long version,
        long lastSeq,
        Instant dbNow) {

    /** Smallest amount an incoming bid must reach. */
    public long minimumBid() {
        return Auction.minimumNextBid(startPrice, minIncrement, currentPrice);
    }
}
