package io.github.mgeladzerezo.auction.auth;

/** An authenticated user, as attached to a REST request or a WebSocket session. */
public record User(long id, String username) {
}
