package io.github.mgeladzerezo.auction;

import io.github.mgeladzerezo.auction.config.AuctionProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/** Entry point of the live auction service. */
@SpringBootApplication
@EnableConfigurationProperties(AuctionProperties.class)
public class LiveAuctionApplication {

    public static void main(String[] args) {
        SpringApplication.run(LiveAuctionApplication.class, args);
    }
}
