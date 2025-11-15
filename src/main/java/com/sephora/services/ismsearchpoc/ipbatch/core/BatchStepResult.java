package com.sephora.services.ismsearchpoc.ipbatch.core;

import com.sephora.services.ismsearchpoc.ipbatch.strategy.InitialConcurrency;
import lombok.Builder;
import lombok.Data;

import java.time.Duration;
import java.time.Instant;

@Data
@Builder
public class BatchStepResult {
    private String stepName;
    private boolean success;
    private Instant startTime;
    private Instant endTime;
    private int itemsProcessed;
    private long recordsProcessed;
    private int errors;
    private String abortReason;
    private InitialConcurrency initialConcurrency;
    
    public boolean shouldAbort() {
        return !success && abortReason != null;
    }
    
    public Duration getDuration() {
        return Duration.between(startTime, endTime);
    }
}
