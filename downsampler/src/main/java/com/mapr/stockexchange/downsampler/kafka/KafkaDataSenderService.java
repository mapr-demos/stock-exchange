package com.mapr.stockexchange.downsampler.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.stockexchange.commons.kafka.AdminService;
import com.mapr.stockexchange.commons.kafka.KafkaClient;
import com.mapr.stockexchange.commons.kafka.util.KafkaNameUtility;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import javax.annotation.PostConstruct;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaDataSenderService {
    private final static String STOCK_STREAM_NAME = "stock";
    private final static String STATISTIC_STREAM_TOPIC_NAME = "statistic:rates";

    private final AdminService adminTopicService;
    private final KafkaClient kafkaClient;
    private final ObjectMapper MAPPER = new ObjectMapper();

    @Value("${kafka.folder}")
    private String kafkaFolder;

    private String dataStreamName;
    private String statisticStreamTopicName;

    @PostConstruct
    public void init() {
        dataStreamName = KafkaNameUtility.convertToKafkaFormat(kafkaFolder, STOCK_STREAM_NAME);
        statisticStreamTopicName = KafkaNameUtility.convertToKafkaFormat(kafkaFolder, STATISTIC_STREAM_TOPIC_NAME);
        adminTopicService.createStreamIfNotExists(dataStreamName);
        adminTopicService.createStreamAndTopicIfNotExists(statisticStreamTopicName);
        log.info("Stock stream: {}", dataStreamName);
        log.info("Statistic topic: {}", statisticStreamTopicName);
    }

    public void createDataTopicIfNotExists(String topic) {
        adminTopicService.createTopicIfNotExists(dataStreamName, topic);
    }

    @SneakyThrows
    public Mono<Void> sendData(String topic, Object data) {
        return kafkaClient.publish(KafkaNameUtility.convertToKafkaTopic(dataStreamName, topic),
                MAPPER.writeValueAsBytes(data));
    }

    @SneakyThrows
    public Mono<Void> sendStatistic(Object data) {
        return kafkaClient.publish(statisticStreamTopicName, MAPPER.writeValueAsBytes(data));
    }

}
