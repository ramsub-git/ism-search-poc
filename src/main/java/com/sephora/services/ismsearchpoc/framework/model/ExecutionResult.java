package com.sephora.services.ismsearchpoc.framework.model;

import java.time.Duration;
import java.time.Instant;

/**
 * Result of an execution run
 */
public class ExecutionResult {
    
    private final boolean success;
    private final String abortReason;
    private final int workItemsProcessed;
    private final int totalWorkItems;
    private final Instant startTime;
    private final Instant endTime;
    
    private ExecutionResult(Builder builder) {
        this.success = builder.success;
        this.abortReason = builder.abortReason;
        this.workItemsProcessed = builder.workItemsProcessed;
        this.totalWorkItems = builder.totalWorkItems;
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
    }
    
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
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private boolean success;
        private String abortReason;
        private int workItemsProcessed;
        private int totalWorkItems;
        private Instant startTime;
        private Instant endTime;
        
        public Builder success(boolean success) {
            this.success = success;
            return this;
        }
        
        public Builder abortReason(String abortReason) {
            this.abortReason = abortReason;
            return this;
        }
        
        public Builder workItemsProcessed(int workItemsProcessed) {
            this.workItemsProcessed = workItemsProcessed;
            return this;
        }
        
        public Builder totalWorkItems(int totalWorkItems) {
            this.totalWorkItems = totalWorkItems;
            return this;
        }
        
        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }
        
        public Builder endTime(Instant endTime) {
            this.endTime = endTime;
            return this;
        }
        
        public ExecutionResult build() {
            return new ExecutionResult(this);
        }
    }
}
