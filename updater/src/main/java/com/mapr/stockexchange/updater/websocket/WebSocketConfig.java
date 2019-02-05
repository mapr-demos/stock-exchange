package com.mapr.stockexchange.updater.websocket;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
@RequiredArgsConstructor
public class WebSocketConfig implements WebSocketConfigurer {

    private final WebSocketStockHandler webSocketStockHandler;
    private final WebSocketStatisticHandler webSocketStatisticHandler;

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(webSocketStockHandler, "/profiles-sockets")
                .setAllowedOrigins("*").withSockJS();
        registry.addHandler(webSocketStatisticHandler, "/statistic-sockets")
                .setAllowedOrigins("*").withSockJS();
    }

}
