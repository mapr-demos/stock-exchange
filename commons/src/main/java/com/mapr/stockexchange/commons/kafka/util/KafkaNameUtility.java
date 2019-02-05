package com.mapr.stockexchange.commons.kafka.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class KafkaNameUtility {

    public String convertToKafkaFormat(String folder, String name) {
        return String.format(folder.endsWith("/") ? "%s%s" : "%s/%s", folder, name);
    }

    public String convertToKafkaTopic(String stream, String topic) {
        return String.format("%s:%s", stream, topic);
    }

}
