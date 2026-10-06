package io.github.mgeladzerezo.auction.realtime;

import io.github.mgeladzerezo.auction.auth.AuthService;
import io.github.mgeladzerezo.auction.auth.User;
import java.util.Map;
import java.util.Optional;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Authenticates the WebSocket handshake.
 *
 * <p>Browsers cannot set headers on a WebSocket request, so the bearer token travels as the
 * {@code token} query parameter. A valid token binds the user to the session for its whole
 * life; a missing token yields a read-only spectator session; an invalid one refuses the
 * handshake with 401 so a client with a stale token finds out immediately.
 */
@Component
public class TokenHandshakeInterceptor implements HandshakeInterceptor {

    private final AuthService auth;

    public TokenHandshakeInterceptor(AuthService auth) {
        this.auth = auth;
    }

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
                                   WebSocketHandler handler, Map<String, Object> attributes) {
        String token = UriComponentsBuilder.fromUri(request.getURI()).build().getQueryParams().getFirst("token");
        if (token == null || token.isBlank()) {
            return true;
        }
        Optional<User> user = auth.authenticate(token);
        if (user.isEmpty()) {
            response.setStatusCode(HttpStatus.UNAUTHORIZED);
            return false;
        }
        attributes.put(AuctionSocketHandler.USER_ATTRIBUTE, user.get());
        return true;
    }

    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response,
                               WebSocketHandler handler, Exception exception) {
        // Nothing to do once the handshake is over.
    }
}
