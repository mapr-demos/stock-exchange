package com.mapr.stockexchange.commons.service;

import com.mapr.stockexchange.commons.entity.DownsamplerConfig;

public interface DownsamplerConfigService {

    DownsamplerConfig getConfig();

    void updateConfig(DownsamplerConfig config);

}
