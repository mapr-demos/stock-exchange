package com.mapr.stockexchange.updater.websocket.entity;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class WebSocketStockDataMessage {
    private String topic;
    private Object data;
}
