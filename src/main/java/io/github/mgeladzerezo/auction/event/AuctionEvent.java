package io.github.mgeladzerezo.auction.event;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.mgeladzerezo.auction.auction.AuctionStatus;
import java.time.Instant;

/**
 * Something that happened to an auction and that every subscriber must learn about, in order.
 *
 * <p>Each event carries the auction's next sequence number. The number is allocated by the same
 * {@code UPDATE} that changes the auction row, so for one auction the order of sequence numbers
 * is the order in which the changes were serialised by the database. Clients use it to detect
 * gaps and to resume after a reconnect.
 *
 * <p>The JSON form (a {@code type} discriminator plus the record components) is both what is
 * stored in {@code auction_events} and what is sent over the WebSocket.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AuctionEvent.Opened.class, name = "AUCTION_OPENED"),
        @JsonSubTypes.Type(value = AuctionEvent.BidAccepted.class, name = "BID_ACCEPTED"),
        @JsonSubTypes.Type(value = AuctionEvent.TimeExtended.class, name = "TIME_EXTENDED"),
        @JsonSubTypes.Type(value = AuctionEvent.Closed.class, name = "AUCTION_CLOSED"),
        @JsonSubTypes.Type(value = AuctionEvent.Settled.class, name = "AUCTION_SETTLED")
})
public sealed interface AuctionEvent {

    long auctionId();

    /** Position in the auction's event stream; starts at 1 and has no gaps. */
    long seq();

    /** Database clock time of the change this event describes. */
    Instant at();

    /** Discriminator as written to the {@code type} column and JSON property. */
    default String typeName() {
        return switch (this) {
            case Opened _ -> "AUCTION_OPENED";
            case BidAccepted _ -> "BID_ACCEPTED";
            case TimeExtended _ -> "TIME_EXTENDED";
            case Closed _ -> "AUCTION_CLOSED";
            case Settled _ -> "AUCTION_SETTLED";
        };
    }

    /** The scheduler moved the auction from SCHEDULED to OPEN. */
    record Opened(long auctionId, long seq, Instant at, Instant endsAt) implements AuctionEvent {
    }

    /** A bid was accepted and its bidder now leads. */
    record BidAccepted(long auctionId, long seq, Instant at, long bidId, long price, long leaderId,
                       String leaderName, int bidCount, long minimumNextBid, Instant endsAt)
            implements AuctionEvent {
    }

    /** The preceding bid landed inside the anti-sniping window and pushed the end time out. */
    record TimeExtended(long auctionId, long seq, Instant at, Instant previousEndsAt, Instant endsAt,
                        int extensionCount, int maxExtensions) implements AuctionEvent {
    }

    /** Bidding is over. {@code winnerId} is null when there were no bids or the reserve was missed. */
    record Closed(long auctionId, long seq, Instant at, Long winnerId, String winnerName,
                  Long finalPrice, boolean reserveMet, int bidCount, Instant endsAt) implements AuctionEvent {
    }

    /** The closed auction passed its audit and reached its final status. */
    record Settled(long auctionId, long seq, Instant at, AuctionStatus status, Long winnerId,
                   String winnerName, Long finalPrice) implements AuctionEvent {
    }
}
