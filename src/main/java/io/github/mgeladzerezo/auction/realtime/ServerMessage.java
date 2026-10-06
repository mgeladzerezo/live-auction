package io.github.mgeladzerezo.auction.realtime;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.mgeladzerezo.auction.auction.AuctionView;
import io.github.mgeladzerezo.auction.bid.BidResult;
import io.github.mgeladzerezo.auction.bid.BidView;
import java.time.Instant;
import java.util.List;

/**
 * Messages the server sends on a socket that are not auction events: answers to the client's
 * own requests. Auction events ({@link io.github.mgeladzerezo.auction.event.AuctionEvent}) and
 * the REPLAY batch share the same {@code type} discriminator convention.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ServerMessage.Welcome.class, name = "WELCOME"),
        @JsonSubTypes.Type(value = ServerMessage.Snapshot.class, name = "SNAPSHOT"),
        @JsonSubTypes.Type(value = ServerMessage.BidAnswer.class, name = "BID_RESULT"),
        @JsonSubTypes.Type(value = ServerMessage.Pong.class, name = "PONG"),
        @JsonSubTypes.Type(value = ServerMessage.Unsubscribed.class, name = "UNSUBSCRIBED"),
        @JsonSubTypes.Type(value = ServerMessage.Failure.class, name = "ERROR")
})
public sealed interface ServerMessage {

    /**
     * First message on every connection.
     *
     * @param instance   which application instance the socket landed on
     * @param userId     the authenticated user, or null for a spectator
     * @param serverTime database time, for the client's clock offset
     */
    record Welcome(String sessionId, String instance, Long userId, String username, Instant serverTime,
                   String strategy) implements ServerMessage {
    }

    /**
     * Complete state of an auction as of sequence number {@code seq}. The client replaces
     * whatever it had and expects {@code seq + 1} next.
     */
    record Snapshot(long auctionId, long seq, Instant serverTime, AuctionView auction, List<BidView> recentBids)
            implements ServerMessage {
    }

    /** The definitive answer to a BID message, correlated by {@code result.clientBidId}. */
    record BidAnswer(BidResult result) implements ServerMessage {
    }

    /** Echoes the client's timestamp next to the server's so the client can estimate its offset. */
    record Pong(Long clientTime, Instant serverTime) implements ServerMessage {
    }

    record Unsubscribed(long auctionId) implements ServerMessage {
    }

    /**
     * A request could not be processed.
     *
     * @param clientBidId set when the failed request was a bid
     * @param retryable   true when a bid's outcome is unknown and the client should re-send it
     *                    with the same {@code clientBidId} to get the definitive answer
     */
    record Failure(String code, String message, String clientBidId, Long auctionId, boolean retryable)
            implements ServerMessage {
    }
}
