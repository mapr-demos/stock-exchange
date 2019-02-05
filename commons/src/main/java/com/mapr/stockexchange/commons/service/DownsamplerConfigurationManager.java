package com.mapr.stockexchange.commons.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.stockexchange.commons.entity.DownsamplerConfig;
import com.mapr.stockexchange.commons.kafka.AdminService;
import com.mapr.stockexchange.commons.kafka.KafkaClient;
import com.mapr.stockexchange.commons.kafka.util.KafkaNameUtility;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class DownsamplerConfigurationManager implements DownsamplerConfigService {
    private final static String DOWNSAMPLER_CONFIG_STREAM_TOPIC_NAME = "downsampler:config";
    private final static long DEFAULT_PERIOD = 3000;

    private final KafkaClient kafkaClient;
    private final AdminService adminService;
    private final ObjectMapper mapper = new ObjectMapper();

    @Value("${kafka.folder}")
    private String kafkaFolder;
    private String fullTopicName;

    private AtomicReference<DownsamplerConfig> config = new AtomicReference<>(new DownsamplerConfig(DEFAULT_PERIOD));

    public DownsamplerConfigurationManager(KafkaClient kafkaClient, AdminService adminService) {
        this.kafkaClient = kafkaClient;
        this.adminService = adminService;
    }

    @PostConstruct
    public void init() {
        fullTopicName = KafkaNameUtility.convertToKafkaFormat(kafkaFolder, DOWNSAMPLER_CONFIG_STREAM_TOPIC_NAME);
        adminService.createStreamAndTopicIfNotExists(fullTopicName);
        log.info("Subscribing for {}", fullTopicName);
        kafkaClient.subscribe(Collections.singleton(fullTopicName)).subscribe(this::setConfig);
    }


    @Override
    public DownsamplerConfig getConfig() {
        return config.get();
    }

    @Override
    public void updateConfig(DownsamplerConfig config) {
        String json;
        try {
            json = mapper.writeValueAsString(config);
            kafkaClient.publish(fullTopicName, json.getBytes()).block();
        } catch (JsonProcessingException e) {
            log.error("Failed to convert config");
        }
    }

    private void setConfig(ConsumerRecord<String,byte[]> record) {
        log.info("Received new Downsampler config");
        try {
            DownsamplerConfig config = mapper.readValue(record.value(), DownsamplerConfig.class);
            this.config.set(config);
            log.info("New config {}", config);
        } catch (IOException e) {
            log.error("Failed to parse {}", new String(record.value()));
        }
    }

}
