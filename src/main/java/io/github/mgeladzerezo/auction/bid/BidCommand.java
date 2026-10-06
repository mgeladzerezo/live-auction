package io.github.mgeladzerezo.auction.bid;

import java.util.regex.Pattern;

/**
 * One bid attempt.
 *
 * @param bidderName  display name of the bidder, carried along so events need no user lookup
 * @param amount      bid in minor units
 * @param clientBidId id chosen by the client; together with the bidder it identifies the attempt,
 *                    so re-sending the same command can never produce a second bid
 */
public record BidCommand(long auctionId, long bidderId, String bidderName, long amount, String clientBidId) {

    /** Largest amount accepted, in minor units. Keeps price + increment far from overflow. */
    public static final long MAX_AMOUNT = 1_000_000_000_000_000L;

    private static final Pattern CLIENT_ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    public BidCommand {
        if (clientBidId == null || !CLIENT_ID.matcher(clientBidId).matches()) {
            throw new IllegalArgumentException("clientBidId must be 1-64 characters of [A-Za-z0-9_-]");
        }
        if (amount <= 0 || amount > MAX_AMOUNT) {
            throw new IllegalArgumentException("amount must be between 1 and " + MAX_AMOUNT + " minor units");
        }
    }
}
