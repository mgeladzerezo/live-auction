package io.github.mgeladzerezo.auction.demo;

import io.github.mgeladzerezo.auction.auth.PasswordHasher;
import io.github.mgeladzerezo.auction.auth.User;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * The accounts the demo needs: one seller for the seeded auctions and a small pool of bots.
 * They are flagged {@code bot} in the users table, which also keeps them out of password login:
 * their password is a random value nobody was ever told.
 */
@Component
class DemoUsers {

    static final String SELLER = "auction-house";
    static final List<String> BOTS = List.of("bot-ada", "bot-grace", "bot-linus", "bot-edsger",
            "bot-barbara", "bot-dennis", "bot-margaret", "bot-ken");

    private final JdbcClient jdbc;
    private final PasswordHasher hasher;

    DemoUsers(JdbcClient jdbc, PasswordHasher hasher) {
        this.jdbc = jdbc;
        this.hasher = hasher;
    }

    User seller() {
        return ensure(List.of(SELLER)).getFirst();
    }

    List<User> bots() {
        return ensure(BOTS);
    }

    private List<User> ensure(List<String> names) {
        String unusablePassword = null;
        for (String name : names) {
            boolean missing = jdbc.sql("SELECT count(*) FROM users WHERE lower(username) = :name")
                    .param("name", name).query(Long.class).single() == 0;
            if (missing) {
                if (unusablePassword == null) {
                    unusablePassword = hasher.hash(UUID.randomUUID().toString());
                }
                jdbc.sql("""
                                INSERT INTO users (username, password_hash, bot) VALUES (:name, :hash, TRUE)
                                ON CONFLICT (lower(username)) DO NOTHING
                                """)
                        .param("name", name).param("hash", unusablePassword).update();
            }
        }
        return jdbc.sql("SELECT id, username FROM users WHERE lower(username) IN (:names) AND bot ORDER BY id")
                .param("names", names)
                .query((rs, row) -> new User(rs.getLong("id"), rs.getString("username")))
                .list();
    }
}
