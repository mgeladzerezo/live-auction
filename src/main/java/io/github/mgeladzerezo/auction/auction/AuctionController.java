package io.github.mgeladzerezo.auction.auction;

import io.github.mgeladzerezo.auction.auth.User;
import io.github.mgeladzerezo.auction.bid.BidCommand;
import io.github.mgeladzerezo.auction.bid.BidHistory;
import io.github.mgeladzerezo.auction.bid.BidResult;
import io.github.mgeladzerezo.auction.bid.BidService;
import io.github.mgeladzerezo.auction.bid.BidView;
import io.github.mgeladzerezo.auction.config.AuctionProperties;
import io.github.mgeladzerezo.auction.config.DbClock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST side of the service. The browser uses it for lists and history and the WebSocket for
 * everything live; bidding is also available here so the system can be driven with curl.
 */
@RestController
@RequestMapping("/api")
public class AuctionController {

    /** Auctions plus the server clock, so a client can align its countdowns from one response. */
    public record AuctionList(Instant serverTime, String instance, List<AuctionView> auctions) {
    }

    public record AuctionDetail(Instant serverTime, AuctionView auction, List<BidView> recentBids) {
    }

    /** Body of {@code POST /api/auctions}. Amounts in minor units, times relative to now. */
    public record CreateAuctionRequest(String title, String description, long startPrice, long minIncrement,
                                       Long reservePrice, Long startsInSeconds, long durationSeconds,
                                       Integer antiSnipeWindowSeconds, Integer maxExtensions) {
    }

    /** Body of {@code POST /api/auctions/{id}/bids}. */
    public record PlaceBidRequest(long amount, String clientBidId) {
    }

    public record ServerInfo(Instant serverTime, String instance, String strategy) {
    }

    private static final int MAX_HISTORY = 500;

    private final AuctionService auctions;
    private final BidService bids;
    private final BidHistory history;
    private final ConsistencyAudit audit;
    private final DbClock clock;
    private final AuctionProperties properties;

    public AuctionController(AuctionService auctions, BidService bids, BidHistory history, ConsistencyAudit audit,
                             DbClock clock, AuctionProperties properties) {
        this.auctions = auctions;
        this.bids = bids;
        this.history = history;
        this.audit = audit;
        this.clock = clock;
        this.properties = properties;
    }

    @GetMapping("/info")
    ServerInfo info() {
        return new ServerInfo(clock.now(), properties.instanceId(), bids.strategyName());
    }

    @GetMapping("/auctions")
    AuctionList list() {
        return new AuctionList(clock.now(), properties.instanceId(),
                auctions.list().stream().map(AuctionView::of).toList());
    }

    @GetMapping("/auctions/{id}")
    AuctionDetail get(@PathVariable long id) {
        Auction auction = auctions.get(id);
        return new AuctionDetail(clock.now(), AuctionView.of(auction),
                history.recent(id, properties.realtime().snapshotBids(), auction.lastSeq()));
    }

    @PostMapping("/auctions")
    @ResponseStatus(HttpStatus.CREATED)
    AuctionView create(User user, @RequestBody CreateAuctionRequest request) {
        Auction created = auctions.create(new NewAuction(
                request.title(), request.description(), user.id(), null,
                request.startPrice(), request.minIncrement(), request.reservePrice(),
                Duration.ofSeconds(request.startsInSeconds() == null ? 0 : request.startsInSeconds()),
                Duration.ofSeconds(request.durationSeconds()),
                request.antiSnipeWindowSeconds() == null ? 30 : request.antiSnipeWindowSeconds(),
                request.maxExtensions() == null ? 10 : request.maxExtensions()));
        return AuctionView.of(created);
    }

    @GetMapping("/auctions/{id}/bids")
    List<BidView> bids(@PathVariable long id, @RequestParam(defaultValue = "50") int limit) {
        auctions.get(id);
        return history.recent(id, Math.clamp(limit, 1, MAX_HISTORY), Long.MAX_VALUE);
    }

    /**
     * Places a bid. Always answers 200 with the definitive result, whether accepted or
     * rejected: a rejected bid is a successful request with a business answer, not an HTTP error.
     */
    @PostMapping("/auctions/{id}/bids")
    BidResult bid(User user, @PathVariable long id, @RequestBody PlaceBidRequest request) {
        return bids.place(new BidCommand(id, user.id(), user.username(), request.amount(), request.clientBidId()));
    }

    /** Runs the consistency audit that settlement uses, on demand. */
    @GetMapping("/auctions/{id}/audit")
    ConsistencyAudit.Report audit(@PathVariable long id) {
        auctions.get(id);
        return audit.check(id);
    }
}
