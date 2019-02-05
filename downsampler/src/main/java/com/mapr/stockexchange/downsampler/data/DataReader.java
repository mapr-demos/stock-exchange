package com.mapr.stockexchange.downsampler.data;

import com.mapr.stockexchange.downsampler.entity.KafkaDataMessage;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;

@Slf4j
class DataReader {

    private final static SimpleDateFormat FORMAT = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    private final String name;
    private List<Path> paths;
    private KafkaDataMessage previousData = null;
    private String startTime;

    public DataReader(File folder, String startTime) {
        name = folder.getName();
        paths = Arrays.stream(Objects.requireNonNull(folder.listFiles()))
                .filter(f -> !f.isDirectory())
                .sorted()
                .map(File::toPath)
                .collect(Collectors.toList());
        this.startTime = startTime.replace(":", "") + "00000";
    }

    Flux<KafkaDataMessage> startParser() {
        log.info("Starting {} from {}", name, paths.stream().map(Path::getFileName).collect(Collectors.toList()));
        Flux<KafkaDataMessage> flux = Flux.empty();

        for (Path path : paths)
            flux = flux.concatWith(getFluxFromPath(path));

        return flux.delayUntil(this::countDelay);
    }

    @SneakyThrows
    private Flux<KafkaDataMessage> getFluxFromPath(Path path) {
        String fileName = path.getFileName().toString();
        String fileDate = fileName.substring(fileName.indexOf("_") + 1, fileName.length() - 4);
        Date startDate = FORMAT.parse(fileDate + startTime);
        return Flux.using(() -> Files.lines(path), Flux::fromStream, BaseStream::close).map(line -> parse(fileDate, line))
                .filter(data -> data.getDate() != null).filter(data -> data.getDate().after(startDate));
    }

    private KafkaDataMessage parse(String date, String line) {
        String[] parts = line.split(Pattern.quote("|"));
        String time = parts[0].substring(0, 9);
        Date actualDate = null;
        try {
            actualDate = FORMAT.parse(date + time);
        } catch (Exception e) {
            log.error("Failed to parse {} {} {}", parts[2], date + time, e.getMessage());
        }

        Double volume = Double.parseDouble(parts[4]);
        Double price = Double.parseDouble(parts[5]);

        return KafkaDataMessage.builder()
                .name(name)
                .date(actualDate)
                .price(price)
                .volume(volume)
                .build();
    }

    private Mono<Integer> countDelay(KafkaDataMessage data) {
        long sleep;

        if (previousData == null)
            sleep = 0;
        else
            sleep = data.getDate().getTime() - previousData.getDate().getTime();

        previousData = data;
        return Mono.just(1).delayElement(Duration.ofMillis(sleep));
    }

}
