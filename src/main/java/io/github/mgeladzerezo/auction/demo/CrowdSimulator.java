package io.github.mgeladzerezo.auction.demo;

import io.github.mgeladzerezo.auction.auction.Auction;
import io.github.mgeladzerezo.auction.auction.AuctionService;
import io.github.mgeladzerezo.auction.auction.AuctionStatus;
import io.github.mgeladzerezo.auction.auth.User;
import io.github.mgeladzerezo.auction.bid.BidCommand;
import io.github.mgeladzerezo.auction.bid.BidResult;
import io.github.mgeladzerezo.auction.bid.BidService;
import io.github.mgeladzerezo.auction.web.ApiException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Makes a demo look alive: a handful of bot bidders raise the price of one auction for a short
 * while. The bots use the ordinary {@link BidService}, so they exercise the same locking,
 * anti-sniping and event path as a human, including being rejected when they lose a race.
 */
@Component
@ConditionalOnProperty(name = "auction.demo.enabled", havingValue = "true")
class CrowdSimulator {

    private static final Logger log = LoggerFactory.getLogger(CrowdSimulator.class);
    private static final Duration RUN_FOR = Duration.ofSeconds(25);
    private static final int BOTS_PER_AUCTION = 5;
    private static final int MAX_CONCURRENT_CROWDS = 4;

    private final BidService bids;
    private final AuctionService auctions;
    private final DemoUsers users;
    private final Set<Long> running = ConcurrentHashMap.newKeySet();

    CrowdSimulator(BidService bids, AuctionService auctions, DemoUsers users) {
        this.bids = bids;
        this.auctions = auctions;
        this.users = users;
    }

    /**
     * Starts bots on an open auction. At most one crowd runs per auction and a few overall, so
     * the button cannot be used to flood the service.
     */
    void start(long auctionId) {
        Auction auction = auctions.get(auctionId);
        if (auction.status() != AuctionStatus.OPEN) {
            throw new ApiException(HttpStatus.CONFLICT, "NOT_OPEN", "Only an open auction can attract a crowd");
        }
        if (running.size() >= MAX_CONCURRENT_CROWDS || !running.add(auctionId)) {
            throw new ApiException(HttpStatus.CONFLICT, "CROWD_BUSY", "A crowd is already bidding here");
        }
        List<User> bots;
        try {
            bots = users.bots().stream().limit(BOTS_PER_AUCTION).toList();
        } catch (RuntimeException e) {
            running.remove(auctionId);
            throw e;
        }
        Thread.startVirtualThread(() -> {
            try {
                run(auctionId, bots);
            } catch (RuntimeException e) {
                log.warn("Crowd on auction {} stopped: {}", auctionId, e.toString());
            } finally {
                running.remove(auctionId);
            }
        });
    }

    private void run(long auctionId, List<User> bots) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long deadline = System.nanoTime() + RUN_FOR.toNanos();
        while (System.nanoTime() < deadline) {
            sleep(300 + random.nextInt(900));
            Auction auction = auctions.get(auctionId);
            if (auction.status() != AuctionStatus.OPEN) {
                return;
            }
            List<User> candidates = bots.stream()
                    .filter(bot -> !Long.valueOf(bot.id()).equals(auction.leaderId())).toList();
            User bot = candidates.get(random.nextInt(candidates.size()));
            long amount = auction.minimumNextBid() + auction.minIncrement() * random.nextInt(0, 3);
            BidResult result = bids.place(new BidCommand(auctionId, bot.id(), bot.username(), amount,
                    "crowd-" + UUID.randomUUID().toString().replace("-", "")));
            log.debug("Crowd bid by {} on {}: {}", bot.username(), auctionId, result);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Crowd interrupted", e);
        }
    }
}
