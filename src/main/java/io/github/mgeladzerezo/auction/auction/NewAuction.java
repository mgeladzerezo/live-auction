package io.github.mgeladzerezo.auction.auction;

import java.time.Duration;

/**
 * Everything needed to create an auction. Times are relative so that the database clock, not
 * the clock of whoever is calling, decides the absolute start and end instants.
 *
 * @param startsIn delay before the auction opens; zero opens it on the next scheduler pass
 * @param duration bidding time before any anti-sniping extension
 */
public record NewAuction(
        String title,
        String description,
        Long sellerId,
        String demoKey,
        long startPrice,
        long minIncrement,
        Long reservePrice,
        Duration startsIn,
        Duration duration,
        int antiSnipeWindowSeconds,
        int maxExtensions) {

    /**
     * Fluent construction with defaults: start price 1000, increment 100, no reserve, opens
     * immediately, runs one hour, anti-sniping off.
     */
    public static final class Builder {

        private String title = "Untitled lot";
        private String description = "";
        private Long sellerId;
        private String demoKey;
        private long startPrice = 1000;
        private long minIncrement = 100;
        private Long reservePrice;
        private Duration startsIn = Duration.ZERO;
        private Duration duration = Duration.ofHours(1);
        private int antiSnipeWindowSeconds;
        private int maxExtensions;

        public Builder title(String title) {
            this.title = title;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder seller(Long sellerId) {
            this.sellerId = sellerId;
            return this;
        }

        public Builder demoKey(String demoKey) {
            this.demoKey = demoKey;
            return this;
        }

        public Builder startPrice(long startPrice) {
            this.startPrice = startPrice;
            return this;
        }

        public Builder minIncrement(long minIncrement) {
            this.minIncrement = minIncrement;
            return this;
        }

        public Builder reservePrice(Long reservePrice) {
            this.reservePrice = reservePrice;
            return this;
        }

        public Builder startsIn(Duration startsIn) {
            this.startsIn = startsIn;
            return this;
        }

        public Builder duration(Duration duration) {
            this.duration = duration;
            return this;
        }

        /** Enables anti-sniping: a bid in the last {@code windowSeconds} adds that much time. */
        public Builder antiSniping(int windowSeconds, int maxExtensions) {
            this.antiSnipeWindowSeconds = windowSeconds;
            this.maxExtensions = maxExtensions;
            return this;
        }

        public NewAuction build() {
            return new NewAuction(title, description, sellerId, demoKey, startPrice, minIncrement, reservePrice,
                    startsIn, duration, antiSnipeWindowSeconds, maxExtensions);
        }
    }
}
