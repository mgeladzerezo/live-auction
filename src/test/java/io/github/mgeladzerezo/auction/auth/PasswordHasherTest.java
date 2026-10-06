package io.github.mgeladzerezo.auction.auth;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.mgeladzerezo.auction.config.AuctionProperties;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class PasswordHasherTest {

    private static PasswordHasher hasher(int iterations) {
        return new PasswordHasher(new AuctionProperties("test", null, null, null,
                new AuctionProperties.Auth(iterations, Duration.ofHours(1)), null));
    }

    @Test
    void verifiesTheRightPasswordOnly() {
        PasswordHasher hasher = hasher(1000);
        String hash = hasher.hash("correct horse");

        assertThat(hasher.matches("correct horse", hash)).isTrue();
        assertThat(hasher.matches("correct horsf", hash)).isFalse();
        assertThat(hasher.matches("", hash)).isFalse();
    }

    @Test
    void saltsEveryHash() {
        PasswordHasher hasher = hasher(1000);
        assertThat(hasher.hash("same")).isNotEqualTo(hasher.hash("same"));
    }

    @Test
    void hashRecordsItsOwnWorkFactorSoItCanBeRaisedLater() {
        String oldHash = hasher(1000).hash("secret-password");

        assertThat(oldHash).startsWith("pbkdf2$1000$");
        assertThat(hasher(5000).matches("secret-password", oldHash)).isTrue();
    }

    @Test
    void placeholderAndMalformedHashesNeverMatch() {
        PasswordHasher hasher = hasher(1000);
        assertThat(hasher.matches("anything", "!")).isFalse();
        assertThat(hasher.matches("anything", null)).isFalse();
        assertThat(hasher.matches("anything", "pbkdf2$1000$not base64$also not")).isFalse();
        assertThat(hasher.matches("anything", "pbkdf2$abc$AAAA$AAAA")).isFalse();
    }
}
