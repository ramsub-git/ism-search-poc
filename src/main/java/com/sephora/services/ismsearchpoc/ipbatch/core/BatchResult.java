package com.sephora.services.ismsearchpoc.ipbatch.core;

import lombok.Builder;
import lombok.Data;
import lombok.Singular;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Data
@Builder
public class BatchResult {
    private String batchName;
    private boolean success;
    private boolean aborted;
    private String abortReason;
    private Instant startTime;
    private Instant endTime;
    
    @Singular
    private List<BatchStepResult> stepResults;
    
    public Duration getDuration() {
        return Duration.between(startTime, endTime);
    }
    
    public int getTotalItemsProcessed() {
        return stepResults.stream()
            .mapToInt(BatchStepResult::getItemsProcessed)
            .sum();
    }
    
    public long getTotalRecordsProcessed() {
        return stepResults.stream()
            .mapToLong(BatchStepResult::getRecordsProcessed)
            .sum();
    }
}
