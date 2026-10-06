package io.github.mgeladzerezo.auction.auction;

import io.github.mgeladzerezo.auction.bid.BidCommand;
import io.github.mgeladzerezo.auction.web.ApiException;
import java.time.Duration;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/** Creating and reading auctions. Validation of new auctions lives here. */
@Service
public class AuctionService {

    private static final Duration MAX_DURATION = Duration.ofDays(30);
    private static final Duration MAX_START_DELAY = Duration.ofDays(30);
    private static final int MAX_ANTI_SNIPE_SECONDS = 3600;
    private static final int MAX_EXTENSIONS = 100;
    private static final int LIST_LIMIT = 100;

    private final AuctionRepository auctions;

    public AuctionService(AuctionRepository auctions) {
        this.auctions = auctions;
    }

    /** Validates and stores a new auction; it opens on the first scheduler pass after its start. */
    public Auction create(NewAuction request) {
        require(request.title() != null && !request.title().isBlank() && request.title().length() <= 120,
                "title is required (at most 120 characters)");
        require(request.description() == null || request.description().length() <= 2000,
                "description may be at most 2000 characters");
        require(request.startPrice() > 0 && request.startPrice() <= BidCommand.MAX_AMOUNT,
                "startPrice must be a positive amount in minor units");
        require(request.minIncrement() > 0 && request.minIncrement() <= BidCommand.MAX_AMOUNT,
                "minIncrement must be a positive amount in minor units");
        require(request.reservePrice() == null
                        || (request.reservePrice() > 0 && request.reservePrice() <= BidCommand.MAX_AMOUNT),
                "reservePrice must be a positive amount in minor units");
        require(!request.startsIn().isNegative() && request.startsIn().compareTo(MAX_START_DELAY) <= 0,
                "startsInSeconds must be between 0 and 30 days");
        require(request.duration().isPositive() && request.duration().compareTo(MAX_DURATION) <= 0,
                "durationSeconds must be positive and at most 30 days");
        require(request.antiSnipeWindowSeconds() >= 0 && request.antiSnipeWindowSeconds() <= MAX_ANTI_SNIPE_SECONDS,
                "antiSnipeWindowSeconds must be between 0 and 3600");
        require(request.maxExtensions() >= 0 && request.maxExtensions() <= MAX_EXTENSIONS,
                "maxExtensions must be between 0 and 100");
        return get(auctions.insert(request));
    }

    public Auction get(long id) {
        return auctions.find(id).orElseThrow(() -> ApiException.notFound("Auction " + id));
    }

    public List<Auction> list() {
        return auctions.list(LIST_LIMIT);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_AUCTION", message);
        }
    }
}
