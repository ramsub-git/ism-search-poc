package com.sephora.services.ismsearchpoc.ipbatch.model;

import java.time.Duration;
import java.time.Instant;
import lombok.Builder;
import lombok.Data;
/**
 * Result of an execution run
 */
@Data
@Builder
public class ExecutionResult {
    
    private final boolean success;
    private final String abortReason;
    private final int workItemsProcessed;
    private final int totalWorkItems;
    private final Instant startTime;
    private final Instant endTime;

    private final long recordsProcessed;
    private final int totalErrors;
    
    public boolean isSuccess() {
        return success;
    }
    
    public String getAbortReason() {
        return abortReason;
    }
    
    public int getWorkItemsProcessed() {
        return workItemsProcessed;
    }
    
    public int getTotalWorkItems() {
        return totalWorkItems;
    }
    
    public Instant getStartTime() {
        return startTime;
    }
    
    public Instant getEndTime() {
        return endTime;
    }
    
    public Duration getDuration() {
        return Duration.between(startTime, endTime);
    }
}
