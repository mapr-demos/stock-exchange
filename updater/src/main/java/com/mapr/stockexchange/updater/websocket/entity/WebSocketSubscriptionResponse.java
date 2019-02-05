package com.mapr.stockexchange.updater.websocket.entity;

import lombok.Data;

@Data
public class WebSocketSubscriptionResponse {
    private SubscriptionMessageType status;
    private String topicName;
    private String topic = "server";
}
