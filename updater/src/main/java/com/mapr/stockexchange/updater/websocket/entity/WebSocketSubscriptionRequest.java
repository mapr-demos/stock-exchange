package com.mapr.stockexchange.updater.websocket.entity;

import lombok.Data;

@Data
public class WebSocketSubscriptionRequest {
    private SubscriptionMessageType type;
    private String topic = "server";
    private String topicName;

}


