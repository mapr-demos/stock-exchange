package com.mapr.stockexchange.updater.controller;

import com.mapr.stockexchange.commons.entity.DownsamplerConfig;
import com.mapr.stockexchange.commons.service.DownsamplerConfigService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@AllArgsConstructor
@RequestMapping(value = "/downsampler")
public class DownsamplerController {

    private DownsamplerConfigService configService;

    @RequestMapping(method = RequestMethod.GET, produces = "application/json")
    public DownsamplerConfig getConfig() {
        log.info("Get request for Downsamplerconfig");
        return configService.getConfig();
    }

    @RequestMapping(method = RequestMethod.POST)
    public void setConfig(@RequestBody DownsamplerConfig config) {
        log.info("Post request for Downsampler");
        configService.updateConfig(config);
    }

}
