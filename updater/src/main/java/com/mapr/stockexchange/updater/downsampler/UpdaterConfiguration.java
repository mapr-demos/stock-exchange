package com.mapr.stockexchange.updater.downsampler;

import com.mapr.stockexchange.commons.kafka.AdminService;
import com.mapr.stockexchange.commons.kafka.KafkaClient;
import com.mapr.stockexchange.commons.service.DownsamplerConfigurationManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UpdaterConfiguration {

    @Bean
    public DownsamplerConfigurationManager kafkaDownsamplerConfigService(KafkaClient kafkaClient, AdminService adminService){
        return new DownsamplerConfigurationManager(kafkaClient, adminService);
    }
}
