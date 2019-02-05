package com.mapr.stockexchange.downsampler.data;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Configuration
public class DataLocatorConfig {

    @Value("${dataset.path}")
    private String path;

    @Bean
    public List<File> files() {
        File file = new File(path);
        List<File> files = Arrays.stream(Objects.requireNonNull(file.listFiles())).filter(File::isDirectory)
                .collect(Collectors.toList());
        log.info("Found {} files in {}", files.size(), path);
        return files;
    }

}
