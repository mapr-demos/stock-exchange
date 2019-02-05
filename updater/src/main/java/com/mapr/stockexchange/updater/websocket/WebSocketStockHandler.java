package com.mapr.stockexchange.updater.websocket;

import com.mapr.stockexchange.commons.entity.StatisticMessage;
import com.mapr.stockexchange.commons.kafka.AdminService;
import com.mapr.stockexchange.commons.kafka.KafkaClient;
import com.mapr.stockexchange.commons.kafka.util.KafkaNameUtility;
import com.mapr.stockexchange.updater.entitiy.Stock;
import com.mapr.stockexchange.updater.profile.ProfileDataService;
import com.mapr.stockexchange.updater.websocket.entity.WebSocketStockDataMessage;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;
import reactor.core.Disposable;

import javax.annotation.PostConstruct;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketStockHandler extends AbstractWebSocketKafkaHandler {
    private final static String STOCK_STREAM_NAME = "stock";
    private final static String STATISTIC_STREAM_TOPIC_NAME = "statistic:subscribers";

    private final KafkaClient kafkaClient;
    private final ProfileDataService dataService;
    private final AdminService adminService;

    @Value("${kafka.folder}")
    private String kafkaFolder;

    @Value("${ui.id:}")
    private String uiId;
    private String stockStreamName;
    private String statisticTopicName;

    @PostConstruct
    public void init() {
        setUiIdIfNecessary();
        stockStreamName = KafkaNameUtility.convertToKafkaFormat(kafkaFolder, STOCK_STREAM_NAME);
        log.info("Stock stream: {}", stockStreamName);
        statisticTopicName = KafkaNameUtility.convertToKafkaFormat(kafkaFolder, STATISTIC_STREAM_TOPIC_NAME);
        log.info("Statistic topic: {}", statisticTopicName);
        adminService.createStreamAndTopicIfNotExists(statisticTopicName);
    }

    @Override
    Disposable subscribe(WebSocketSession session, String profileName) {
        Set<String> topics = dataService.getStocksFromProfile(profileName).stream().map(Stock::getName)
                .map(topic -> KafkaNameUtility.convertToKafkaTopic(stockStreamName, topic)).collect(Collectors.toSet());

        return kafkaClient.subscribe(topics).subscribe(record -> convertAndSend(session, profileName, record));
    }

    @Override
    @SneakyThrows
    protected void onSubscribersAmountChange() {
        StatisticMessage msg = getStatisticMessage();
        byte[] data = mapper.writeValueAsBytes(msg);
        kafkaClient.publish(statisticTopicName, data).subscribe();
    }

    private StatisticMessage getStatisticMessage() {
        return StatisticMessage.builder().uiId(uiId).subscribersAmount(getSubscribersAmount()).build();
    }

    @SneakyThrows
    private void convertAndSend(WebSocketSession session, String topic, ConsumerRecord<String,byte[]> record) {
        Object data = mapper.readValue(record.value(), Object.class);
        WebSocketStockDataMessage msg = WebSocketStockDataMessage.builder().topic(topic).data(data).build();
        sendMessage(session, msg);
    }

    private void setUiIdIfNecessary() {
        if(uiId.equals(""))
            uiId = UUID.randomUUID().toString().replace("-", "");
        log.info("UI ID is {}", uiId);
    }

}
