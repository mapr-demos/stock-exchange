package com.mapr.stockexchange.downsampler.data;

import com.mapr.stockexchange.commons.entity.StatisticMessage;
import com.mapr.stockexchange.downsampler.entity.KafkaDataMessage;
import org.apache.commons.lang3.time.StopWatch;
import reactor.core.publisher.Flux;

import java.util.Optional;

public class DataStatisticCounter {
    private final static int RATE_PERIOD = 1000;
    private final StopWatch timer = new StopWatch();
    private int counter = 0;

    public Flux<StatisticMessage> apply(Flux<KafkaDataMessage> flux) {
        timer.start();
        return flux.map(this::convertToStatisticMessage).filter(Optional::isPresent).map(Optional::get);
    }

    private Optional<StatisticMessage> convertToStatisticMessage(KafkaDataMessage data) {
        Optional<StatisticMessage> result = Optional.empty();

        long time = timer.getTime();
        if(time >= RATE_PERIOD) {
            result = Optional.of(StatisticMessage.builder()
                    .symbol(data.getName())
                    .incomingRate(((double)++counter) * RATE_PERIOD / time)
                    .build());
            counter = 0;
            restartTimer();
        } else {
            counter++;
        }

        return result;
    }

    private void restartTimer() {
        timer.reset();
        timer.start();
    }

}
