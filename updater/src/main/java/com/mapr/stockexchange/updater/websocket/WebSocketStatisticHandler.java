package com.mapr.stockexchange.updater.websocket;

import com.mapr.stockexchange.commons.kafka.KafkaClient;
import com.mapr.stockexchange.commons.kafka.util.KafkaNameUtility;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;
import reactor.core.Disposable;

import javax.annotation.PostConstruct;
import java.util.HashSet;
import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketStatisticHandler extends AbstractWebSocketKafkaHandler {
    private final static String RATES_STREAM_TOPIC_NAME = "statistic:rates";
    private final static String SUBSCRIBERS_STREAM_TOPIC_NAME = "statistic:subscribers";

    private final KafkaClient kafkaClient;

    @Value("${kafka.folder}")
    private String kafkaFolder;
    private Set<String> statisticTopics;

    @PostConstruct
    private void init() {
        statisticTopics = new HashSet<>();
        statisticTopics.add(KafkaNameUtility.convertToKafkaFormat(kafkaFolder, SUBSCRIBERS_STREAM_TOPIC_NAME));
        statisticTopics.add(KafkaNameUtility.convertToKafkaFormat(kafkaFolder, RATES_STREAM_TOPIC_NAME));
    }

    @Override
    Disposable subscribe(WebSocketSession session, String topic) {
        return kafkaClient.subscribe(statisticTopics).subscribe(data -> processAndSend(session, data));
    }

    private void processAndSend(WebSocketSession session, ConsumerRecord<String,byte[]> record) {
        sendMessage(session, record.value());
    }

}
