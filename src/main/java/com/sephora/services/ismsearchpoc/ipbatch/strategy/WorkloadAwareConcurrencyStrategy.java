package com.sephora.services.ismsearchpoc.ipbatch.strategy;

import com.sephora.services.ismsearchpoc.ipbatch.sizing.WorkloadAnalysis;
import com.sephora.services.ismsearchpoc.ipbatch.sizing.WorkloadSize;

public class WorkloadAwareConcurrencyStrategy implements ConcurrencyStrategy {
    
    private final int smallBatchThreshold;
    private final int mediumBatchThreshold;
    
    private WorkloadAwareConcurrencyStrategy(int smallBatchThreshold, int mediumBatchThreshold) {
        this.smallBatchThreshold = smallBatchThreshold;
        this.mediumBatchThreshold = mediumBatchThreshold;
    }
    
    @Override
    public InitialConcurrency calculate(
            WorkloadAnalysis workload,
            ConcurrencyLimits limits,
            ResourceSnapshot resources) {
        
        WorkloadSize size = workload.categorize();
        
        int workItemConcurrency = switch (size) {
            case SMALL -> limits.getMinWorkItemConcurrency();
            case MEDIUM -> (limits.getMinWorkItemConcurrency() + limits.getMaxWorkItemConcurrency()) / 2;
            case LARGE -> (int) (limits.getMaxWorkItemConcurrency() * 0.8);
        };
        
        int processingConcurrency = switch (size) {
            case SMALL -> limits.getMinProcessingConcurrency();
            case MEDIUM -> (limits.getMinProcessingConcurrency() + limits.getMaxProcessingConcurrency()) / 2;
            case LARGE -> (int) (limits.getMaxProcessingConcurrency() * 0.8);
        };
        
        int safeDbConnections = (int) (resources.getAvailableDbConnections() * 0.7);
        workItemConcurrency = Math.min(workItemConcurrency, safeDbConnections);
        
        String rationale = String.format(
            "Workload: %s (%d items, %d records) → workItem=%d, processing=%d",
            size, workload.getWorkItemCount(), workload.getTotalRecords(),
            workItemConcurrency, processingConcurrency
        );
        
        return InitialConcurrency.builder()
            .workItemConcurrency(workItemConcurrency)
            .processingConcurrency(processingConcurrency)
            .rationale(rationale)
            .build();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private int smallBatchThreshold = 50;
        private int mediumBatchThreshold = 500;
        
        public Builder smallBatchThreshold(int threshold) {
            this.smallBatchThreshold = threshold;
            return this;
        }
        
        public Builder mediumBatchThreshold(int threshold) {
            this.mediumBatchThreshold = threshold;
            return this;
        }
        
        public WorkloadAwareConcurrencyStrategy build() {
            return new WorkloadAwareConcurrencyStrategy(smallBatchThreshold, mediumBatchThreshold);
        }
    }
}
