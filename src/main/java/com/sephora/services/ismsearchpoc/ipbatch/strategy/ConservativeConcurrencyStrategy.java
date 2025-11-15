package com.sephora.services.ismsearchpoc.ipbatch.strategy;

import com.sephora.services.ismsearchpoc.ipbatch.sizing.WorkloadAnalysis;

public class ConservativeConcurrencyStrategy implements ConcurrencyStrategy {
    
    @Override
    public InitialConcurrency calculate(
            WorkloadAnalysis workload,
            ConcurrencyLimits limits,
            ResourceSnapshot resources) {
        
        return InitialConcurrency.builder()
            .workItemConcurrency(limits.getMinWorkItemConcurrency())
            .processingConcurrency(limits.getMinProcessingConcurrency())
            .rationale("Conservative: Starting with minimum concurrency")
            .build();
    }
}
