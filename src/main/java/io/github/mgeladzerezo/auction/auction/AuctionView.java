package io.github.mgeladzerezo.auction.auction;

import java.time.Instant;

/**
 * What clients see of an auction, over REST and in WebSocket snapshots. The reserve price is
 * deliberately not exposed: only whether one exists and whether it has been met.
 *
 * @param seq sequence number of the last event already reflected in this view
 */
public record AuctionView(
        long id,
        String title,
        String description,
        AuctionStatus status,
        long startPrice,
        long minIncrement,
        Long currentPrice,
        long minimumNextBid,
        Long leaderId,
        String leaderName,
        int bidCount,
        Instant startsAt,
        Instant endsAt,
        Instant originalEndsAt,
        int antiSnipeWindowSeconds,
        int extensionCount,
        int maxExtensions,
        boolean hasReserve,
        boolean reserveMet,
        Long sellerId,
        long seq) {

    public static AuctionView of(Auction a) {
        return new AuctionView(a.id(), a.title(), a.description(), a.status(), a.startPrice(),
                a.minIncrement(), a.currentPrice(), a.minimumNextBid(), a.leaderId(), a.leaderName(),
                a.bidCount(), a.startsAt(), a.endsAt(), a.originalEndsAt(), a.antiSnipeWindowSeconds(),
                a.extensionCount(), a.maxExtensions(), a.reservePrice() != null, a.reserveMet(),
                a.sellerId(), a.lastSeq());
    }
}
