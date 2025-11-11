package com.sephora.services.ismsearchpoc.processing;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ism.processing.logging")
@Data
public class ProcessingLoggingProperties {
    private String serviceName;
    private SeverityLevel defaultSeverity = SeverityLevel.P1;
    private boolean auditEnabled = true;
    private boolean errorLoggingEnabled = true;
    private RetryConfig retry = new RetryConfig();

    @Data
    public static class RetryConfig {
        private long backoffIntervalMs = 5000L;
        private int maxRetries = 4;
        private boolean logAllRetries = false;
    }
}