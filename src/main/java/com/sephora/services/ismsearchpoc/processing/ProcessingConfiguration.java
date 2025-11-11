package com.sephora.services.ismsearchpoc.processing;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(ProcessingLoggingProperties.class)
@Slf4j
public class ProcessingConfiguration {

    public ProcessingConfiguration(ProcessingLoggingProperties properties) {
        log.info("Processing framework initialized for service: {}", properties.getServiceName());
        log.info("Audit enabled: {}, Error logging enabled: {}",
                properties.isAuditEnabled(), properties.isErrorLoggingEnabled());
    }
}