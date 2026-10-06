package io.github.mgeladzerezo.auction.auth;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/** Register, log in, and look up the current user. */
@RestController
@RequestMapping("/api/auth")
public class AuthController {

    /** Request body for both register and login. */
    public record Credentials(String username, String password) {
    }

    /** A bearer token and who it belongs to. */
    public record TokenResponse(String token, long userId, String username) {

        static TokenResponse of(AuthService.Session session) {
            return new TokenResponse(session.token(), session.user().id(), session.user().username());
        }
    }

    private final AuthService auth;

    public AuthController(AuthService auth) {
        this.auth = auth;
    }

    @PostMapping("/register")
    @ResponseStatus(HttpStatus.CREATED)
    TokenResponse register(@RequestBody Credentials credentials) {
        return TokenResponse.of(auth.register(credentials.username(), credentials.password()));
    }

    @PostMapping("/login")
    TokenResponse login(@RequestBody Credentials credentials) {
        return TokenResponse.of(auth.login(credentials.username(), credentials.password()));
    }

    @GetMapping("/me")
    User me(User user) {
        return user;
    }
}
