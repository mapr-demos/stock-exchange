package com.mapr.stockexchange.downsampler.entity;

import lombok.Builder;
import lombok.Data;
import java.util.Date;

@Data
@Builder
public class KafkaDataMessage {
    private String name;
    private Date date;
    private Double price;
    private Double volume;
}
