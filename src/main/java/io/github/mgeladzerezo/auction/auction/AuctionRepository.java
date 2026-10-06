package io.github.mgeladzerezo.auction.auction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Reads and creates auction rows. Bidding and lifecycle transitions have their own SQL. */
@Repository
public class AuctionRepository {

    private static final String SELECT = """
            SELECT a.id, a.title, a.description, a.seller_id, a.demo_key, a.start_price,
                   a.min_increment, a.reserve_price, a.starts_at, a.ends_at, a.original_ends_at,
                   a.anti_snipe_window_seconds, a.max_extensions, a.extension_count, a.status,
                   a.current_price, a.leader_id, u.username AS leader_name, a.bid_count,
                   a.version, a.last_seq, a.closed_at
              FROM auctions a
              LEFT JOIN users u ON u.id = a.leader_id
            """;

    private final JdbcClient jdbc;

    public AuctionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts an auction in SCHEDULED state. Start and end instants are computed by the
     * database from its own clock.
     */
    public long insert(NewAuction a) {
        return jdbc.sql("""
                        INSERT INTO auctions (title, description, seller_id, demo_key, start_price,
                                              min_increment, reserve_price, starts_at, ends_at,
                                              original_ends_at, anti_snipe_window_seconds, max_extensions)
                        SELECT :title, :description, CAST(:sellerId AS bigint), CAST(:demoKey AS text),
                               :startPrice, :minIncrement, CAST(:reservePrice AS bigint),
                               t.starts, t.starts + :durationMs * interval '1 millisecond',
                               t.starts + :durationMs * interval '1 millisecond', :window, :maxExtensions
                          FROM (SELECT clock_timestamp() + :startsInMs * interval '1 millisecond' AS starts) t
                        RETURNING id
                        """)
                .param("title", a.title())
                .param("description", a.description() == null ? "" : a.description())
                .param("sellerId", a.sellerId())
                .param("demoKey", a.demoKey())
                .param("startPrice", a.startPrice())
                .param("minIncrement", a.minIncrement())
                .param("reservePrice", a.reservePrice())
                .param("durationMs", a.duration().toMillis())
                .param("startsInMs", a.startsIn().toMillis())
                .param("window", a.antiSnipeWindowSeconds())
                .param("maxExtensions", a.maxExtensions())
                .query(Long.class)
                .single();
    }

    public Optional<Auction> find(long id) {
        return jdbc.sql(SELECT + " WHERE a.id = :id").param("id", id).query(AuctionRepository::map).optional();
    }

    /**
     * Auctions for the list page: live ones first (soonest to end on top), then upcoming, then
     * the most recently finished.
     */
    public List<Auction> list(int limit) {
        return jdbc.sql(SELECT + """
                         ORDER BY CASE a.status WHEN 'OPEN' THEN 0 WHEN 'SCHEDULED' THEN 1 ELSE 2 END,
                                  CASE WHEN a.status IN ('OPEN', 'SCHEDULED') THEN a.ends_at END ASC,
                                  a.ends_at DESC
                         LIMIT :limit
                        """)
                .param("limit", limit)
                .query(AuctionRepository::map)
                .list();
    }

    static Auction map(ResultSet rs, int row) throws SQLException {
        return new Auction(
                rs.getLong("id"),
                rs.getString("title"),
                rs.getString("description"),
                rs.getObject("seller_id", Long.class),
                rs.getString("demo_key"),
                rs.getLong("start_price"),
                rs.getLong("min_increment"),
                rs.getObject("reserve_price", Long.class),
                instant(rs, "starts_at"),
                instant(rs, "ends_at"),
                instant(rs, "original_ends_at"),
                rs.getInt("anti_snipe_window_seconds"),
                rs.getInt("max_extensions"),
                rs.getInt("extension_count"),
                AuctionStatus.valueOf(rs.getString("status")),
                rs.getObject("current_price", Long.class),
                rs.getObject("leader_id", Long.class),
                rs.getString("leader_name"),
                rs.getInt("bid_count"),
                rs.getLong("version"),
                rs.getLong("last_seq"),
                instant(rs, "closed_at"));
    }

    /** Reads a {@code timestamptz} column as an {@link Instant}, keeping microsecond precision. */
    public static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }
}
