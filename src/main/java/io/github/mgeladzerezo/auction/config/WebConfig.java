package io.github.mgeladzerezo.auction.config;

import io.github.mgeladzerezo.auction.auth.CurrentUserResolver;
import io.github.mgeladzerezo.auction.realtime.AuctionSocketHandler;
import io.github.mgeladzerezo.auction.realtime.TokenHandshakeInterceptor;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

/** Wires the WebSocket endpoint, the bearer-token argument resolver and scheduling. */
@Configuration
@EnableWebSocket
@EnableScheduling
public class WebConfig implements WebSocketConfigurer, WebMvcConfigurer {

    /** Largest client message accepted. Client messages are tiny; this caps abuse. */
    private static final int MAX_INBOUND_MESSAGE_BYTES = 4096;

    private final AuctionSocketHandler socketHandler;
    private final TokenHandshakeInterceptor handshakeInterceptor;
    private final CurrentUserResolver currentUserResolver;

    public WebConfig(AuctionSocketHandler socketHandler, TokenHandshakeInterceptor handshakeInterceptor,
                     CurrentUserResolver currentUserResolver) {
        this.socketHandler = socketHandler;
        this.handshakeInterceptor = handshakeInterceptor;
        this.currentUserResolver = currentUserResolver;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        // Default origin policy: same-origin browsers and non-browser clients only.
        registry.addHandler(socketHandler, "/ws").addInterceptors(handshakeInterceptor);
    }

    @Override
    public void addArgumentResolvers(List<HandlerMethodArgumentResolver> resolvers) {
        resolvers.add(currentUserResolver);
    }

    @Bean
    ServletServerContainerFactoryBean webSocketContainer() {
        ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
        container.setMaxTextMessageBufferSize(MAX_INBOUND_MESSAGE_BYTES);
        container.setMaxBinaryMessageBufferSize(MAX_INBOUND_MESSAGE_BYTES);
        return container;
    }
}
