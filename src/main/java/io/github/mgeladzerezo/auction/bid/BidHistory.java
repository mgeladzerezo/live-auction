package io.github.mgeladzerezo.auction.bid;

import io.github.mgeladzerezo.auction.auction.AuctionRepository;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Read side of the append-only bid history. */
@Repository
public class BidHistory {

    private final JdbcClient jdbc;

    public BidHistory(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * The most recent accepted bids, newest first.
     *
     * @param upToSeq only bids whose event sequence number is at most this value. A snapshot
     *                passes the auction's {@code last_seq} it read, so that a bid committed a
     *                moment later is delivered once (as an event) and not twice.
     */
    public List<BidView> recent(long auctionId, int limit, long upToSeq) {
        return jdbc.sql("""
                        SELECT b.id, b.seq, b.bidder_id, u.username, b.amount, b.accepted_at, b.extended_to
                          FROM bids b
                          JOIN users u ON u.id = b.bidder_id
                         WHERE b.auction_id = :auctionId AND b.seq <= :upToSeq
                         ORDER BY b.seq DESC
                         LIMIT :limit
                        """)
                .param("auctionId", auctionId)
                .param("upToSeq", upToSeq)
                .param("limit", limit)
                .query((rs, row) -> new BidView(rs.getLong("id"), rs.getLong("seq"), rs.getLong("bidder_id"),
                        rs.getString("username"), rs.getLong("amount"),
                        AuctionRepository.instant(rs, "accepted_at"), rs.getObject("extended_to") != null))
                .list();
    }
}
