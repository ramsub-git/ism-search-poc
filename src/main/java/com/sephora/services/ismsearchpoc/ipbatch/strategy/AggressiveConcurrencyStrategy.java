package com.sephora.services.ismsearchpoc.ipbatch.strategy;

import com.sephora.services.ismsearchpoc.ipbatch.sizing.WorkloadAnalysis;

public class AggressiveConcurrencyStrategy implements ConcurrencyStrategy {
    
    @Override
    public InitialConcurrency calculate(
            WorkloadAnalysis workload,
            ConcurrencyLimits limits,
            ResourceSnapshot resources) {
        
        int workItemConcurrency = Math.min(
            limits.getMaxWorkItemConcurrency(),
            (int) (resources.getAvailableDbConnections() * 0.8)
        );
        
        return InitialConcurrency.builder()
            .workItemConcurrency(workItemConcurrency)
            .processingConcurrency(limits.getMaxProcessingConcurrency())
            .rationale("Aggressive: Starting with maximum safe concurrency")
            .build();
    }
}
