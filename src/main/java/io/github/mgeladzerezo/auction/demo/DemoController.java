package io.github.mgeladzerezo.auction.demo;

import io.github.mgeladzerezo.auction.auth.User;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/** The "simulate crowd" button. Only exists when the demo is enabled. */
@RestController
@RequestMapping("/api/demo")
@ConditionalOnProperty(name = "auction.demo.enabled", havingValue = "true")
class DemoController {

    private final CrowdSimulator crowd;

    DemoController(CrowdSimulator crowd) {
        this.crowd = crowd;
    }

    /** Requires a logged-in user, like bidding does; the user is not otherwise involved. */
    @PostMapping("/auctions/{id}/crowd")
    @ResponseStatus(HttpStatus.ACCEPTED)
    void simulateCrowd(User user, @PathVariable long id) {
        crowd.start(id);
    }
}
