package com.mapr.stockexchange.downsampler.data;

import com.mapr.stockexchange.commons.entity.StatisticMessage;
import com.mapr.stockexchange.commons.service.DownsamplerConfigurationManager;
import com.mapr.stockexchange.downsampler.entity.KafkaDataMessage;
import com.mapr.stockexchange.downsampler.kafka.KafkaDataSenderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class DataGeneratorService {

    private final KafkaDataSenderService sender;
    private final DownsamplerConfigurationManager configService;
    private final List<File> stockFiles;

    @Value("${dataset.time.start:00:00}")
    private String startTime;

    @PostConstruct
    public void generateAndSend() {
        log.info("Starting time: " + startTime);
        stockFiles.stream().map(File::getName).forEach(sender::createDataTopicIfNotExists);
        List<DataReader> readers = stockFiles.stream().map(folder -> new DataReader(folder, startTime))
                .collect(Collectors.toList());
        readers.forEach(this::startDataSender);
    }

    private void startDataSender(DataReader reader) {
        DataWindow windowCollector = new DataWindow((Void) -> configService.getConfig().getPeriod());
        DataStatisticCounter statisticCounter = new DataStatisticCounter();
        Flux<KafkaDataMessage> dataInput = reader.startParser().share();
        windowCollector.apply(Flux.from(dataInput)).concatMap(this::sendData).subscribe();
        statisticCounter.apply(Flux.from(dataInput)).concatMap(this::sendStatistic).subscribe();
    }

    private Mono<Void> sendData(KafkaDataMessage data) {
        return sender.sendData(data.getName(), data);
    }

    private Mono<Void> sendStatistic(StatisticMessage data) {
        return sender.sendStatistic(data);
    }

}


