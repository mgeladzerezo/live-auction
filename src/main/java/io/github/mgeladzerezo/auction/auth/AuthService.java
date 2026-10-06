package io.github.mgeladzerezo.auction.auth;

import io.github.mgeladzerezo.auction.config.AuctionProperties;
import io.github.mgeladzerezo.auction.web.ApiException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Optional;
import java.util.regex.Pattern;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

/**
 * Registration, login and token checks.
 *
 * <p>Deliberately small: opaque random bearer tokens, stored hashed, expiring on the database
 * clock. One token authenticates both REST calls ({@code Authorization: Bearer}) and the
 * WebSocket handshake ({@code ?token=}).
 */
@Service
public class AuthService {

    /** A logged-in user together with the freshly issued token. */
    public record Session(String token, User user) {
    }

    private static final Pattern USERNAME = Pattern.compile("[A-Za-z0-9_.-]{3,32}");
    private static final int MIN_PASSWORD_LENGTH = 8;
    private static final int MAX_PASSWORD_LENGTH = 200;

    private final JdbcClient jdbc;
    private final PasswordHasher hasher;
    private final SecureRandom random = new SecureRandom();
    private final long tokenTtlSeconds;
    /** Hash checked when the user does not exist, so both failure paths cost the same time. */
    private final String decoyHash;

    public AuthService(JdbcClient jdbc, PasswordHasher hasher, AuctionProperties properties) {
        this.jdbc = jdbc;
        this.hasher = hasher;
        this.tokenTtlSeconds = properties.auth().tokenTtl().toSeconds();
        this.decoyHash = hasher.hash("decoy-password");
    }

    public Session register(String username, String password) {
        if (username == null || !USERNAME.matcher(username).matches()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_USERNAME",
                    "Username must be 3-32 characters: letters, digits, dot, dash or underscore");
        }
        if (password == null || password.length() < MIN_PASSWORD_LENGTH || password.length() > MAX_PASSWORD_LENGTH) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_PASSWORD",
                    "Password must be %d-%d characters".formatted(MIN_PASSWORD_LENGTH, MAX_PASSWORD_LENGTH));
        }
        try {
            long id = jdbc.sql("INSERT INTO users (username, password_hash) VALUES (:username, :hash) RETURNING id")
                    .param("username", username)
                    .param("hash", hasher.hash(password))
                    .query(Long.class)
                    .single();
            User user = new User(id, username);
            return new Session(issueToken(user), user);
        } catch (DuplicateKeyException e) {
            throw new ApiException(HttpStatus.CONFLICT, "USERNAME_TAKEN", "That username is already registered");
        }
    }

    public Session login(String username, String password) {
        record Credentials(long id, String username, String hash) {
        }
        Optional<Credentials> found = username == null ? Optional.empty() : jdbc.sql(
                        "SELECT id, username, password_hash FROM users WHERE lower(username) = lower(:username) AND NOT bot")
                .param("username", username)
                .query((rs, row) -> new Credentials(rs.getLong("id"), rs.getString("username"), rs.getString("password_hash")))
                .optional();
        String candidate = password == null ? "" : password;
        boolean valid = hasher.matches(candidate, found.map(Credentials::hash).orElse(decoyHash)) && found.isPresent();
        if (!valid) {
            throw new ApiException(HttpStatus.UNAUTHORIZED, "BAD_CREDENTIALS", "Wrong username or password");
        }
        User user = new User(found.get().id(), found.get().username());
        return new Session(issueToken(user), user);
    }

    /** Resolves a bearer token to its user if the token exists and has not expired. */
    public Optional<User> authenticate(String token) {
        if (token == null || token.isBlank() || token.length() > 200) {
            return Optional.empty();
        }
        return jdbc.sql("""
                        SELECT u.id, u.username
                          FROM auth_tokens t JOIN users u ON u.id = t.user_id
                         WHERE t.token_hash = :hash AND t.expires_at > clock_timestamp()
                        """)
                .param("hash", sha256(token))
                .query((rs, row) -> new User(rs.getLong("id"), rs.getString("username")))
                .optional();
    }

    private String issueToken(User user) {
        byte[] bytes = new byte[32];
        random.nextBytes(bytes);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        jdbc.sql("""
                        INSERT INTO auth_tokens (token_hash, user_id, expires_at)
                        VALUES (:hash, :userId, clock_timestamp() + :ttl * interval '1 second')
                        """)
                .param("hash", sha256(token))
                .param("userId", user.id())
                .param("ttl", tokenTtlSeconds)
                .update();
        return token;
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
