package io.github.mgeladzerezo.auction.event;

import java.time.Instant;

/**
 * An event in its wire form: the routing fields the hub needs, plus the JSON that is forwarded
 * to clients untouched.
 */
public record StoredEvent(long auctionId, long seq, Instant at, String json) {
}
