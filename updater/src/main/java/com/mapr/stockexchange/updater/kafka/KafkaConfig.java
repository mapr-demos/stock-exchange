package com.mapr.stockexchange.updater.kafka;

import com.mapr.stockexchange.commons.kafka.AdminService;
import com.mapr.stockexchange.commons.kafka.KafkaClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Bean
    public AdminService adminService() {
        return new AdminService();
    }

    @Bean
    public KafkaClient kafkaClient() {
        return new KafkaClient();
    }

}
