package com.mapr.stockexchange.downsampler.data;

import com.mapr.stockexchange.downsampler.entity.KafkaDataMessage;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.time.StopWatch;
import reactor.core.publisher.Flux;

import java.util.Optional;
import java.util.function.Function;

@RequiredArgsConstructor
public class DataWindow {
    private final Function<Void, Long> periodGetter;

    private final StopWatch timer = new StopWatch();
    private KafkaDataMessage last;

    Flux<KafkaDataMessage> apply(Flux<KafkaDataMessage> source) {
        return source.map(this::getPreviousDataAndEmitLatestWindow).filter(Optional::isPresent).map(Optional::get);
    }

    private Optional<KafkaDataMessage> getPreviousDataAndEmitLatestWindow(KafkaDataMessage event) {
        Optional<KafkaDataMessage> result = Optional.empty();

        if(!timer.isStarted()) {
            timer.start();
            last = event;
            result = Optional.of(event);
        } else {
            if (timer.getTime() == periodGetter.apply(null)) {
                result = Optional.of(event);
                restartTimer();
            } else if (timer.getTime() > periodGetter.apply(null)) {
                result = Optional.of(last);
                restartTimer();
            }

            last = event;
        }

        return result;
    }

    private void restartTimer() {
        timer.reset();
        timer.start();
    }
}
