package com.mapr.stockexchange.commons.entity;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class StatisticMessage {
    private String uiId;
    private Integer subscribersAmount;
    private String symbol;
    private Double incomingRate;
}
