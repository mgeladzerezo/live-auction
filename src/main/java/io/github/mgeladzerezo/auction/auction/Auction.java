package io.github.mgeladzerezo.auction.auction;

import java.time.Instant;

/**
 * An auction row as read from the database. Amounts are integer minor units.
 *
 * @param currentPrice amount of the leading bid, or {@code null} before the first bid
 * @param version      optimistic-lock token, incremented by every change to the row
 * @param lastSeq      sequence number of the last event published for this auction
 */
public record Auction(
        long id,
        String title,
        String description,
        Long sellerId,
        String demoKey,
        long startPrice,
        long minIncrement,
        Long reservePrice,
        Instant startsAt,
        Instant endsAt,
        Instant originalEndsAt,
        int antiSnipeWindowSeconds,
        int maxExtensions,
        int extensionCount,
        AuctionStatus status,
        Long currentPrice,
        Long leaderId,
        String leaderName,
        int bidCount,
        long version,
        long lastSeq,
        Instant closedAt) {

    /** The smallest amount the next bid must reach. */
    public long minimumNextBid() {
        return minimumNextBid(startPrice, minIncrement, currentPrice);
    }

    /** Shared rule: the opening bid must reach the start price, later bids price + increment. */
    public static long minimumNextBid(long startPrice, long minIncrement, Long currentPrice) {
        return currentPrice == null ? startPrice : currentPrice + minIncrement;
    }

    /** Whether the leading bid satisfies the reserve; false when there is no bid. */
    public boolean reserveMet() {
        return reserveMet(currentPrice, reservePrice);
    }

    /** Shared rule: an auction with a bid and no reserve always meets it. */
    public static boolean reserveMet(Long currentPrice, Long reservePrice) {
        return currentPrice != null && (reservePrice == null || currentPrice >= reservePrice);
    }
}
